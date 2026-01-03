import os
import io
import requests
import fitz  # PyMuPDF
import cloudinary.uploader
from dotenv import load_dotenv

from worker.utils import update_state
from ..worker import celery_app

load_dotenv("../../.env")

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

@celery_app.task(name="pdf.extract")
def extract_pdf_task(
    task_id: str,
    file_url: str,
    start_page: int,
    end_page: int,
):
    """
    Extract pages from a PDF and upload the extracted PDF to Cloudinary.
    Page numbers are 1-based (user-friendly).
    """

    try:
        update_state(task_id, status="processing", progress=5)

        # Download original PDF
        response = requests.get(file_url, timeout=60)
        response.raise_for_status()

        update_state(task_id, progress=15)

        original_pdf = fitz.open(stream=response.content, filetype="pdf")
        total_pages = original_pdf.page_count

        # Validate page range
        if start_page < 1 or end_page > total_pages or start_page > end_page:
            raise ValueError("Invalid page range")

        update_state(task_id, progress=30)

        # Create new PDF
        extracted_pdf = fitz.open()

        # Convert to 0-based index
        extracted_pdf.insert_pdf(
            original_pdf,
            from_page=start_page - 1,
            to_page=end_page - 1
        )

        update_state(task_id, progress=60)

        # Save extracted PDF SAFELY
        extracted_buffer = io.BytesIO()
        extracted_pdf.save(
            extracted_buffer,
            garbage=4,
            deflate=True,
            clean=True
        )

        extracted_buffer.seek(0)
        extracted_pdf.close()
        original_pdf.close()

        # 🔍 Safety check
        extracted_size = len(extracted_buffer.getvalue())
        if extracted_size == 0:
            raise RuntimeError("Extracted PDF is empty")

        update_state(task_id, progress=80)

        # Upload to Cloudinary (IMPORTANT FIX)
        extracted_upload = cloudinary.uploader.upload(
            extracted_buffer.getvalue(),
            folder="mediaforge/pdf_extracted",
            resource_type="raw",
            public_id=f"{task_id}.pdf"
        )

        update_state(
            task_id,
            status="completed",
            progress="100",
            result_url=extracted_upload["secure_url"],
        )

        return extracted_upload["secure_url"]

    except Exception as e:
        update_state(task_id, status="failed", error=str(e))
        raise
