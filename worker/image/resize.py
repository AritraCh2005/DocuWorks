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

@celery_app.task(name='image.resize')
def resize_image_task(task_id, file_url, width, height, maintain_aspect_ratio=True):
    """Resize image task"""
    try:
        print(f"Starting image resize task {task_id}")
        
        update_state(task_id, status="processing", progress=10)
        
        # Download image
        response = requests.get(file_url)
        response.raise_for_status()

        update_state(task_id, progress=30)

        # Open image
        image = Image.open(io.BytesIO(response.content))
        original_format = image.format

        update_state(task_id, progress=50)
        
        # Resize image
        if maintain_aspect_ratio:
            image.thumbnail((width, height), Image.Resampling.LANCZOS)
        else:
            image = image.resize((width, height), Image.Resampling.LANCZOS)

        update_state(task_id, progress=70)

        # Save resized image
        resized_buffer = io.BytesIO()
        save_format = original_format if original_format else "PNG"
        
        save_kwargs = {"format": save_format}
        if save_format == "JPEG":
            save_kwargs["quality"] = 95
            save_kwargs["optimize"] = True
            if image.mode in ("RGBA", "P"):
                image = image.convert("RGB")
        
        image.save(resized_buffer, **save_kwargs)
        resized_buffer.seek(0)

        update_state(task_id, progress=85)

        # Upload resized image
        resized_upload = cloudinary.uploader.upload(
            resized_buffer.getvalue(),
            folder="mediaforge/resized",
            format=save_format.lower()
        )

        result_data = {
            "status": "completed",
            "progress": "100",
            "result_url": resized_upload["secure_url"]
        }
        
        update_state(task_id, **result_data)
        
        print(f"Completed image resize task {task_id}")
        return resized_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in image resize task {task_id}: {e}")
        update_state(task_id, status="failed", error=str(e))
        raise e