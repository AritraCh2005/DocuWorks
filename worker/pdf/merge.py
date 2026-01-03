import os
import cloudinary.uploader
import redis
import io
import requests
from dotenv import load_dotenv

from worker.utils import update_state
from ..worker import celery_app
try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF not installed. Install with: pip install PyMuPDF")

load_dotenv("../../.env")

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

@celery_app.task(name='pdf.merge')
def merge_pdf_task(task_id, pdf_urls):
    """Merge multiple PDFs task"""
    try:
        print(f"Starting PDF merge task {task_id}")
        
        update_state(task_id, status="processing", progress=10)
        
        if len(pdf_urls) < 2:
            raise ValueError("At least 2 PDF files are required for merging")
        
        # Create new PDF document for merged result
        merged_pdf = fitz.open()
        
        total_pdfs = len(pdf_urls)
        progress_per_pdf = 70 / total_pdfs
        
        for i, pdf_url in enumerate(pdf_urls):
            # Download PDF
            response = requests.get(pdf_url)
            response.raise_for_status()
            
            # Open PDF
            pdf_document = fitz.open(stream=response.content, filetype="pdf")
            
            # Insert all pages from this PDF
            merged_pdf.insert_pdf(pdf_document)
            
            pdf_document.close()
            
            # Update progress
            current_progress = 10 + (i + 1) * progress_per_pdf
            update_state(task_id, progress=int(current_progress))

        update_state(task_id, progress=85)

        # Save merged PDF
        merged_buffer = io.BytesIO()
        merged_pdf.save(merged_buffer)
        merged_buffer.seek(0)
        
        merged_pdf.close()

        update_state(task_id, progress=90)

        # Upload merged PDF
        merged_upload = cloudinary.uploader.upload(
            merged_buffer.getvalue(),
            folder="mediaforge/pdf_merged",
            resource_type="raw",
            format="pdf"
        )

        result_data = {
            "status": "completed",
            "progress": "100",
            "result_url": merged_upload["secure_url"],
        }

        update_state(task_id, **result_data)
        
        print(f"Completed PDF merge task {task_id}")
        return merged_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in PDF merge task {task_id}: {e}")
        redis_client.hset(task_id, mapping={
            "status": "failed",
            "error": str(e)
        })
        raise e