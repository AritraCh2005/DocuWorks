import os
import uuid
import json
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import cloudinary.uploader
import cloudinary.api
import redis.asyncio as redis
from dotenv import load_dotenv
from celery import Celery

load_dotenv()


app = FastAPI(title="MediaForge API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Redis client (async)
redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

# Celery client (only used to push tasks)
celery_app = Celery('worker', broker=os.getenv("CELERY_BROKER_URL"))

# Helper function to upload file and create task
async def create_task(task_name: str, upload: UploadFile, **kwargs):
    task_id = str(uuid.uuid4())
    
    try:
        # Read file bytes
        file_bytes = await upload.read()
        
        # Determine resource type based on file type
        resource_type = "raw" if upload.content_type == "application/pdf" else "auto"
        
        # Upload original file to Cloudinary
        uploaded = cloudinary.uploader.upload(
            file_bytes,
            folder="mediaforge/originals",
            resource_type=resource_type
        )
        original_url = uploaded["secure_url"]
        
        # Push task to celery worker
        celery_app.send_task(
            task_name,
            kwargs={
                "task_id": task_id,
                "file_url": original_url,
                **kwargs
            }
        )
        
        # Initialize task status in Redis
        await redis_client.hset(task_id, mapping={"status": "queued", "progress": "0"})
        
        return task_id
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload or task failed: {e}")

@app.get("/")
async def root():
    return {"message": "MediaForge API is running", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    try:
        await redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "redis": "disconnected", "error": str(e)}

# Image processing endpoints
@app.post("/image/compress")
async def compress_image_task(upload: UploadFile = File(...), 
                                  quality: int = Form(80), 
                                  target_size_kb: Optional[int] = Form(None),
                                  preserve_format: bool = Form(True),
                                  ):
    """Compress an image with specified quality"""
    if not upload.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    if quality < 1 or quality > 100:
        raise HTTPException(status_code=400, detail="Quality must be between 1 and 100")
    
    task_args = {
        "quality": quality,
        "preserve_format": preserve_format,
    }
    if target_size_kb is not None:
        task_args["target_size_kb"] = target_size_kb

    task_id = await create_task(
        "image.compress",
        upload,
        **task_args
    )
    
    return {"task_id": task_id}

@app.post("/image/resize")
async def resize_image(
    upload: UploadFile = File(...),
    width: int = Form(...),
    height: int = Form(...),
    maintain_aspect_ratio: bool = Form(True)
):
    """Resize an image to specified dimensions"""
    if not upload.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    if width <= 0 or height <= 0:
        raise HTTPException(status_code=400, detail="Width and height must be positive")
    
    task_id = await create_task(
        "image.resize",
        upload,
        width=width,
        height=height,
        maintain_aspect_ratio=maintain_aspect_ratio
    )
    
    return {"task_id": task_id}

@app.post("/image/convert")
async def convert_image(
    upload: UploadFile = File(...),
    target_format: str = Form(...)
):
    """Convert an image to a different format"""
    if not upload.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    valid_formats = ["jpg", "jpeg", "png", "webp", "bmp", "tiff", "gif", "pdf"]
    if target_format.lower() not in valid_formats:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid format. Must be one of: {', '.join(valid_formats)}"
        )
    
    task_id = await create_task(
        "image.convert",
        upload,
        target_format=target_format.lower()
    )
    
    return {"task_id": task_id}

# PDF processing endpoints
@app.post("/pdf/compress")
async def compress_pdf(
    upload: UploadFile = File(...),
    compression_level: str = Form("medium")
):
    """Compress a PDF file"""
    if upload.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    valid_levels = ["low", "medium", "high"]
    if compression_level not in valid_levels:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid compression level. Must be one of: {', '.join(valid_levels)}"
        )
    task_args = {
        "compression_level": compression_level,
    }

    task_id = await create_task(
        "pdf.compress",
        upload,
        **task_args,
    )
    
    return {"task_id": task_id}

@app.post("/pdf/merge")
async def merge_pdfs(files: list[UploadFile] = File(...)):
    """Merge multiple PDF files into one"""
    if len(files) < 2:
        raise HTTPException(status_code=400, detail="At least 2 PDF files are required for merging")
    
    # Validate all files are PDFs
    for file in files:
        if file.content_type != "application/pdf":
            raise HTTPException(status_code=400, detail="All files must be PDFs")
    
    task_id = str(uuid.uuid4())
    
    try:
        # Upload all PDFs to Cloudinary
        pdf_urls = []
        for file in files:
            file_bytes = await file.read()
            uploaded = cloudinary.uploader.upload(
                file_bytes,
                folder="mediaforge/originals",
                resource_type="raw"
            )
            pdf_urls.append(uploaded["secure_url"])
        
        # Push task to celery worker
        celery_app.send_task(
            "pdf.merge",
            kwargs={
                "task_id": task_id,
                "pdf_urls": pdf_urls
            }
        )
        
        # Initialize task status in Redis
        await redis_client.hset(task_id, mapping={"status": "queued", "progress": "0"})
        
        return {"task_id": task_id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload or task failed: {e}")

@app.post("/pdf/extract")
async def extract_pdf_pages(
    upload: UploadFile = File(...),
    start_page: int = Form(...),
    end_page: int = Form(...),
):
    """Extract pages or text from a PDF"""
    if upload.content_type != "application/pdf":
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    task_id = await create_task(
        "pdf.extract",
        upload,
        start_page=start_page,
        end_page=end_page,
    )
    
    return {"task_id": task_id}

# Task status and management endpoints
@app.get("/progress/{task_id}")
async def get_progress(task_id: str):
    """Get the progress and status of a task"""
    status_data = await redis_client.hgetall(task_id)
    if not status_data:
        return {"status": "unknown", "message": "Task ID not found"}

    # Return status, progress, and result_url if available
    response = {
        "status": status_data.get("status"),
        "progress": status_data.get("progress", "0"),
    }
    if "result_url" in status_data:
        response["result_url"] = status_data["result_url"]
    if "error" in status_data:
        response["error"] = status_data["error"]

    return response

@app.websocket("/ws/progress/{task_id}")
async def websocket_progress(websocket: WebSocket, task_id: str):
    await websocket.accept()

    # Send latest known state immediately
    current_state = await redis_client.hgetall(task_id)
    if current_state:
        response = {
            "status": current_state.get("status"),
            "progress": current_state.get("progress", "0"),
        }
        if "result_url" in current_state:
            response["result_url"] = current_state["result_url"]
    if "error" in current_state:
        response["error"] = current_state["error"]
    await websocket.send_text(json.dumps(response))

    # Start pubsub stream
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"progress:{task_id}")
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            await websocket.send_text(message["data"])

            data = json.loads(message["data"])
            if data.get("status") in {"completed", "failed", "cancelled"}:
                break
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(f"progress:{task_id}")
        await pubsub.close()
        await websocket.close()

@app.delete("/task/{task_id}")
async def cancel_task(task_id: str):
    """Cancel a processing task"""
    status_data = await redis_client.hgetall(task_id)
    
    if not status_data:
        raise HTTPException(status_code=404, detail="Task not found")
    
    current_status = status_data.get("status")
    if current_status in ["completed", "failed"]:
        raise HTTPException(status_code=400, detail="Cannot cancel completed or failed task")
    
    # Revoke the Celery task
    celery_app.control.revoke(task_id, terminate=True)
    
    # Update task status in Redis
    await redis_client.hset(task_id, "status", "cancelled")
    
    return {"message": "Task cancelled successfully"}

@app.get("/tasks")
async def list_tasks(limit: int = 10, offset: int = 0):
    """List recent tasks with pagination"""
    try:
        # Get all task keys
        task_keys = await redis_client.keys("*")
        # Filter out non-UUID keys (in case Redis has other data)
        task_keys = [key for key in task_keys if len(key) == 36 and key.count('-') == 4]
        
        # Apply pagination
        total_tasks = len(task_keys)
        paginated_keys = task_keys[offset:offset + limit]
        
        tasks = []
        for task_id in paginated_keys:
            task_data = await redis_client.hgetall(task_id)
            if task_data:
                tasks.append({
                    "task_id": task_id,
                    "status": task_data.get("status", "unknown"),
                    "progress": task_data.get("progress", "0")
                })
        
        return {
            "tasks": tasks,
            "total": total_tasks,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve tasks: {e}")

@app.delete("/cleanup/cloudinary")
async def cleanup_cloudinary():
    """
    Delete all files from Cloudinary except for those still being processed.
    """
    try:
        # Fetch all task IDs
        task_keys = await redis_client.keys("*")
        task_ids = [key for key in task_keys if len(key) == 36 and key.count('-') == 4]

        # Keep track of URLs to preserve (still in progress)
        urls_to_preserve = set()

        for task_id in task_ids:
            data = await redis_client.hgetall(task_id)
            status = data.get("status")
            file_url = data.get("file_url") or data.get("result_url")

            # If still running, preserve the associated file URL
            if status in ["queued", "in_progress"] and file_url:
                urls_to_preserve.add(file_url)

        # List all files in the "mediaforge/originals" and "mediaforge/results" folders
        deleted_resources = []

        for folder in ["mediaforge/originals", "mediaforge/compressed", "mediaforge/converted", "mediaforge/resized"]:
            # Likely images, so resource_type="image"
            resources = cloudinary.api.resources(type="upload", prefix=folder, max_results=500)
            for resource in resources.get("resources", []):
                if resource["secure_url"] not in urls_to_preserve:
                    cloudinary.uploader.destroy(resource["public_id"], invalidate=True)
                    deleted_resources.append(resource["public_id"])

        # For folders with PDFs and raw files:
        for folder in ["mediaforge/pdf_compressed", "mediaforge/pdf_extracted", "mediaforge/pdf_merged"]:
            resources = cloudinary.api.resources(type="upload", resource_type="raw", prefix=folder, max_results=500)
            for resource in resources.get("resources", []):
                if resource["secure_url"] not in urls_to_preserve:
                    cloudinary.uploader.destroy(resource["public_id"], resource_type="raw", invalidate=True)
                    deleted_resources.append(resource["public_id"])

        return {
            "message": "Cleanup complete",
            "deleted_files": deleted_resources,
            "preserved_files": list(urls_to_preserve)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {e}")
