# worker/pdf_compress.py

import os
import json
import tempfile
import subprocess
import requests
import shutil
import platform

import pikepdf
import redis
import cloudinary
import cloudinary.uploader as cu

from dotenv import load_dotenv
from ..worker import celery_app

# ---------- ENV ----------
load_dotenv("../../.env")

# ---------- REDIS ----------
REDIS = redis.from_url(
    os.environ["CELERY_BROKER_URL"],
    decode_responses=True
)

# ---------- CLOUDINARY ----------
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

# ---------- COMPRESSION LEVELS ----------
LEVELS = {
    "low": {
        "dpi": 300,
        "jpeg_q": 85,
        "jbig2": "--jbig2-lossless",
        "gs": "/printer",
    },
    "medium": {
        "dpi": 150,
        "jpeg_q": 65,
        "jbig2": "--jbig2-lossless",
        "gs": "/ebook",
    },
    "high": {
        "dpi": 96,
        "jpeg_q": 45,
        "jbig2": "--jbig2-lossy",
        "gs": "/screen",
    },
}

# ---------- HELPERS ----------

def _update(task_id, **fields):
    """Update Redis task state."""
    REDIS.hset(task_id, mapping={k: str(v) for k, v in fields.items()})
    REDIS.publish(f"progress:{task_id}", json.dumps(fields))

def _safe_path(path):
    """
    Ghostscript hates Windows backslashes in paths (e.g., C:\Temp).
    We must convert them to forward slashes.
    """
    if platform.system() == "Windows":
        return path.replace("\\", "/")
    return path

def _validate_file(path, step_name):
    """Raises error if file doesn't exist or is empty."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"[{step_name}] File not found: {path}")
    size = os.path.getsize(path)
    if size == 0:
        raise RuntimeError(f"[{step_name}] Generated file is 0 bytes: {path}")
    return size

def _is_scanned(pdf: pikepdf.Pdf, sample=3) -> bool:
    """
    Checks if pages have images but NO fonts (text).
    """
    scanned_cnt = 0
    pages = pdf.pages[:sample]
    if not pages: return False
    
    for page in pages:
        # Check if page has images
        has_images = len(page.images) > 0
        # Check if page has fonts (selectable text)
        has_fonts = "/Font" in page.resources
        
        if has_images and not has_fonts:
            scanned_cnt += 1
            
    return (scanned_cnt / len(pages)) >= 0.8

def _find_executable(unix_name, windows_alt=None):
    if os.name == "nt" and windows_alt:
        path = shutil.which(windows_alt)
        if path: return path
    path = shutil.which(unix_name)
    if not path:
        raise FileNotFoundError(f"Executable not found: {unix_name}")
    return path

# ---------- RESOLVE BINARIES ----------
GS_EXEC = _find_executable("gs", windows_alt="gswin64c")
OCR_EXEC = shutil.which("ocrmypdf")

# ---------- CELERY TASK ----------
@celery_app.task(name="pdf.compress", bind=True, max_retries=2)
def compress_pdf_task(self, task_id: str, file_url: str, compression_level="medium"):
    try:
        if compression_level not in LEVELS:
            compression_level = "medium"
        cfg = LEVELS[compression_level]

        _update(task_id, status="processing", progress=5)

        # 1. DOWNLOAD
        resp = requests.get(file_url, timeout=60)
        resp.raise_for_status()
        pdf_bytes = resp.content
        orig_size = len(pdf_bytes)
        
        if orig_size == 0:
            raise ValueError("Source file download is 0 bytes.")

        _update(task_id, progress=20)

        with tempfile.TemporaryDirectory() as tmp:
            # Setup paths
            src = os.path.join(tmp, "src.pdf")
            p1 = os.path.join(tmp, "p1.pdf")
            p2 = os.path.join(tmp, "p2.pdf")
            out = os.path.join(tmp, "out.pdf")

            # Write Source
            with open(src, "wb") as f:
                f.write(pdf_bytes)
            _validate_file(src, "Download")

            _update(task_id, progress=30)

            # 2. PRE-PROCESS (Pikepdf)
            # IMPORTANT: Do NOT use linearize=True here. It breaks GS processing often.
            with pikepdf.open(src) as pdf:
                scanned = _is_scanned(pdf)
                pdf.save(p1, compress_streams=True)
            
            _validate_file(p1, "Pikepdf")

            # 3. OCR (Conditional)
            p2_in = p1
            if scanned and OCR_EXEC:
                _update(task_id, progress=45)
                ocr_cmd = [
                    OCR_EXEC,
                    "--skip-text",
                    "--optimize", "2",
                    "--jpeg-quality", str(cfg["jpeg_q"]),
                    "--image-dpi", str(cfg["dpi"]),
                    cfg["jbig2"],
                    "--output-type", "pdf",
                    p1,
                    p2,
                ]
                subprocess.run(ocr_cmd, check=True)
                _validate_file(p2, "OCR")
                p2_in = p2

            # 4. GHOSTSCRIPT COMPRESSION
            _update(task_id, progress=70)

            # Sanitize paths for Ghostscript (Crucial for Windows)
            gs_input = _safe_path(p2_in)
            gs_output = _safe_path(out)
            gs_exe = _safe_path(GS_EXEC)

            gs_cmd = [
                gs_exe,
                "-sDEVICE=pdfwrite",
                "-dCompatibilityLevel=1.4",
                f"-dPDFSETTINGS={cfg['gs']}",
                "-dNOPAUSE", 
                "-dQUIET", 
                "-dBATCH",
                "-dSAFER",
                f"-sOutputFile={gs_output}",
                gs_input,
            ]
            
            # Run GS
            subprocess.run(gs_cmd, check=True)
            
            # Validate Final Output
            final_size = _validate_file(out, "Ghostscript")
            
            # Calculate Savings
            ratio = 100 * (1 - final_size / orig_size)
            _update(task_id, progress=85)

            # 5. UPLOAD
            # Use resource_type="auto" to let Cloudinary detect it as a PDF correctly.
            # "raw" is risky if headers aren't perfect.
            upload_res = cu.upload(
                out,
                resource_type="auto",  
                folder="mediaforge/pdf_compressed",
                use_filename=True,
                unique_filename=True,
                flags="attachment"  # Optional: forces browser to download rather than view
            )
            
            cloud_url = upload_res["secure_url"]

        # DONE
        _update(
            task_id,
            status="completed",
            progress=100,
            result_url=cloud_url,
            original_kb=f"{orig_size / 1024:.1f}",
            compressed_kb=f"{final_size / 1024:.1f}",
            saving=f"{ratio:.1f}%",
        )

        return cloud_url

    except subprocess.CalledProcessError as e:
        _update(task_id, status="failed", error=f"Process Error: {e}")
        raise

    except Exception as e:
        _update(task_id, status="failed", error=f"Worker Error: {str(e)}")
        raise