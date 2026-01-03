import os
import cloudinary.uploader
import redis
from PIL import Image, ImageOps
import io
import requests
import pillow_heif
from pillow_avif import AvifImagePlugin
from typing import Tuple, Optional
from dotenv import load_dotenv

from worker.utils import update_state
from ..worker import celery_app

load_dotenv("../../.env")

# Register HEIF opener with Pillow
pillow_heif.register_heif_opener()

# Configure Cloudinary
cloudinary.config(
    cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
    api_key=os.getenv("CLOUDINARY_API_KEY"),
    api_secret=os.getenv("CLOUDINARY_API_SECRET"),
)

# Initialize Redis client
redis_client = redis.from_url(os.getenv("CELERY_BROKER_URL"), decode_responses=True)

class ImageCompressor:
    """Image compression using multiple algorithms"""
    
    @staticmethod
    def get_optimal_format_and_quality(image: Image.Image, target_size_kb: Optional[int] = None) -> Tuple[str, int, dict]:
        """Determine optimal format and quality based on image characteristics"""
        width, height = image.size
        has_transparency = image.mode in ('RGBA', 'LA') or 'transparency' in image.info
        pixel_count = width * height
        
        # For transparent images
        if has_transparency:
            if pixel_count > 1000000:  # Large images with transparency
                return "WEBP", 85, {"method": 6}
            else:
                return "PNG", 95, {"optimize": True}
        
        # For non-transparent images
        if pixel_count > 2000000:  # Very large images
            return "AVIF", 75, {"quality": 75, "speed": 6}
        elif pixel_count > 500000:  # Large images
            return "WEBP", 80, {"method": 6}
        else:  # Smaller images
            return "JPEG", 85, {"optimize": True, "progressive": True}
    
    @staticmethod
    def compress_with_jpeg(image: Image.Image, quality: int = 85) -> io.BytesIO:
        """Compress using optimized JPEG settings"""
        buffer = io.BytesIO()
        
        # Convert to RGB if needed
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        # Use PIL's optimized JPEG compression
        image.save(buffer, 'JPEG', quality=quality, optimize=True, progressive=True)
        buffer.seek(0)
        return buffer
    
    @staticmethod
    def compress_with_webp(image: Image.Image, quality: int = 80) -> io.BytesIO:
        """Compress using WebP with optimal settings"""
        buffer = io.BytesIO()
        
        # WebP supports both lossy and lossless
        if image.mode in ('RGBA', 'LA') or 'transparency' in image.info:
            # Use lossless for images with transparency if quality is high
            if quality > 90:
                image.save(buffer, 'WEBP', lossless=True, method=6)
            else:
                image.save(buffer, 'WEBP', quality=quality, method=6)
        else:
            image.save(buffer, 'WEBP', quality=quality, method=6, optimize=True)
        
        buffer.seek(0)
        return buffer
    
    @staticmethod
    def compress_with_avif(image: Image.Image, quality: int = 75) -> io.BytesIO:
        """Compress using AVIF for maximum compression efficiency"""
        buffer = io.BytesIO()
        
        # AVIF provides excellent compression
        image.save(buffer, 'AVIF', quality=quality, speed=6)
        buffer.seek(0)
        return buffer
    
    @staticmethod
    def compress_with_jxl(image: Image.Image, quality: int = 85) -> io.BytesIO:
        """Compress using JPEG XL (if available)"""
        try:
            # This requires pillow-jxl-plugin
            buffer = io.BytesIO()
            image.save(buffer, 'JXL', quality=quality)
            buffer.seek(0)
            return buffer
        except Exception:
            raise Exception("JPEG XL compression not available")
      
    @staticmethod
    def optimize_image_preprocessing(image: Image.Image) -> Image.Image:
        """Optimize image before compression"""
        # Auto-orient based on EXIF
        image = ImageOps.exif_transpose(image)
        
        # Remove unnecessary metadata while preserving essential info
        if hasattr(image, '_getexif') and image._getexif():
            # Keep only essential EXIF data
            essential_tags = {274, 306, 315}  # Orientation, DateTime, Artist
            exif_dict = {}
            try:
                for tag_id, value in image._getexif().items():
                    if tag_id in essential_tags:
                        exif_dict[tag_id] = value
            except:
                pass
        
        return image

@celery_app.task(name='image.compress')
def compress_image_task(
    task_id: str, 
    file_url: str, 
    compression_method: str = "auto",
    quality: int = 75,
    target_size_kb: Optional[int] = None,
    preserve_format: bool = True,
):
    """
    Advanced image compression task with multiple algorithms
    
    Args:
        task_id: Unique task identifier
        file_url: URL of source image
        compression_method: "auto", "jpeg", "webp", "avif", "jxl", "png"
        quality: Compression quality (1-100)
        target_size_kb: Target file size in KB
        preserve_format: Whether to keep original format
    """
    try:
        print(f"Starting advanced image compression task {task_id}")
        compressor = ImageCompressor()
        
        # Update status
        update_state(task_id, status="processing", progress=5)
        
        # Download image
        response = requests.get(file_url, timeout=30)
        response.raise_for_status()

        update_state(task_id, progress=15)

        # Open image
        original_image = Image.open(io.BytesIO(response.content))
        original_format = original_image.format
        original_size_kb = len(response.content) / 1024
        
        print(f"Original image: {original_image.size}, Format: {original_format}, Size: {original_size_kb:.1f}KB")

        update_state(task_id, progress=25)

        # Preprocessing
        image = compressor.optimize_image_preprocessing(original_image)

        update_state(task_id, progress=35)

        # Determine compression method
        if preserve_format and original_format in ['JPEG', 'PNG', 'WEBP']:
            target_format = original_format
            format_params = {"optimize": True}
        elif compression_method == "auto":
            target_format, quality, format_params = compressor.get_optimal_format_and_quality(
                image, target_size_kb
            )
        else:
            target_format = compression_method.upper()
            format_params = {}

        update_state(task_id, progress=55)

        # Compress with selected method
        compressed_buffer = None
        
        try:
            if target_format == "JPEG":
                compressed_buffer = compressor.compress_with_jpeg(image, quality)
            elif target_format == "WEBP":
                compressed_buffer = compressor.compress_with_webp(image, quality)
            elif target_format == "AVIF":
                compressed_buffer = compressor.compress_with_avif(image, quality)
            elif target_format == "JXL":
                compressed_buffer = compressor.compress_with_jxl(image, quality)
            else:
                # Standard PIL compression
                compressed_buffer = io.BytesIO()
                
                if target_format == "PNG":
                    image.save(compressed_buffer, "PNG", optimize=True, **format_params)
                else:
                    # Fallback to JPEG
                    if image.mode in ("RGBA", "P", "LA"):
                        image = image.convert("RGB")
                    image.save(compressed_buffer, "JPEG", quality=quality, optimize=True, progressive=True)
                
                compressed_buffer.seek(0)
                
        except Exception as e:
            print(f"Compression method failed, falling back to JPEG: {e}")
            compressed_buffer = compressor.compress_with_jpeg(image, quality)

        update_state(task_id, progress=75)

        # Check if target size is met, adjust quality if needed
        compressed_size_kb = len(compressed_buffer.getvalue()) / 1024
        
        if target_size_kb and compressed_size_kb > target_size_kb and quality > 30:
            # Iteratively reduce quality to meet target size
            attempts = 3
            while compressed_size_kb > target_size_kb and quality > 30 and attempts > 0:
                quality -= 15
                
                if target_format == "JPEG":
                    compressed_buffer = compressor.compress_with_jpeg(image, quality)
                else:
                    # Use WebP for better compression at lower qualities
                    compressed_buffer = compressor.compress_with_webp(image, quality)
                
                compressed_size_kb = len(compressed_buffer.getvalue()) / 1024
                attempts -= 1

        update_state(task_id, progress=85)

        # Upload to Cloudinary
        file_extension = target_format.lower() if target_format != "JPEG" else "jpg"
        
        compressed_upload = cloudinary.uploader.upload(
            compressed_buffer.getvalue(),
            folder="mediaforge/compressed",
            format=file_extension,
            resource_type="image"
        )
        
        # Calculate compression ratio
        final_size_kb = len(compressed_buffer.getvalue()) / 1024
        compression_ratio = (1 - final_size_kb / original_size_kb) * 100
        
        # Store results
        result_data = {
            "status": "completed",
            "progress": "100",
            "result_url": compressed_upload["secure_url"],
            "original_size_kb": f"{original_size_kb:.1f}",
            "compressed_size_kb": f"{final_size_kb:.1f}",
            "compression_ratio": f"{compression_ratio:.1f}%",
            "format": target_format,
            "quality": str(quality)
        }
        
        update_state(task_id, **result_data)
        
        print(f"Completed compression: {original_size_kb:.1f}KB → {final_size_kb:.1f}KB ({compression_ratio:.1f}% reduction)")
        return compressed_upload["secure_url"]
        
    except Exception as e:
        print(f"Error in advanced compression task {task_id}: {e}")
        update_state(task_id, status="failed", error=str(e))
        raise e