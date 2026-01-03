import os
import cloudinary.uploader
import redis
from PIL import Image
import io
import requests
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

redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

@celery_app.task(name='image.convert')
def convert_image_task(task_id, file_url, target_format="PNG"):
    """Convert image format task"""

    PILLOW_FORMATS = {
        "jpg": "JPEG",
        "jpeg": "JPEG",
        "png": "PNG",
        "webp": "WEBP",
        "bmp": "BMP",
        "tiff": "TIFF",
        "tif": "TIFF",
        "gif": "GIF",
        "ico": "ICO",
        "pdf": "PDF",
    }

    try:
        print(f"Starting image conversion task {task_id}")
        
        update_state(task_id, status="processing", progress=10)
        
        # Download image
        response = requests.get(file_url)
        response.raise_for_status()

        update_state(task_id, progress=30)

        # Open image
        image = Image.open(io.BytesIO(response.content))
        
        # Handle different format conversions
        if target_format.upper() == "JPEG" and image.mode in ("RGBA", "P"):
            image = image.convert("RGB")
        elif target_format.upper() == "PNG" and image.mode != "RGBA":
            image = image.convert("RGBA")
        elif target_format.upper() == "PDF" and image.mode != "RGB":
            image = image.convert("RGB")

        update_state(task_id, progress=60)

        # Convert image
        converted_buffer = io.BytesIO()
        normalized_format = PILLOW_FORMATS.get(target_format.lower())
        if not normalized_format:
            raise ValueError(f"Unsupported format: {target_format}")

        save_kwargs = {}

        if normalized_format == "JPEG":
            save_kwargs["quality"] = 95
            save_kwargs["optimize"] = True

        image.save(converted_buffer, format=normalized_format, **save_kwargs)
        converted_buffer.seek(0)

        update_state(task_id, progress=80)

        # Upload converted image
        converted_upload = cloudinary.uploader.upload(
            converted_buffer.getvalue(),
            folder="mediaforge/converted",
            format=target_format.lower(),
            resource_type="raw" if target_format.lower() == "pdf" else "image"
        )
        
        result_data = {
            "status": "completed",
            "progress": "100",
            "result_url": converted_upload["secure_url"],
        }

        update_state(task_id, **result_data)
        
        print(f"Completed image conversion task {task_id}")
        return converted_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in image conversion task {task_id}: {e}")
        update_state(task_id, status="failed", error=str(e))
        raise e