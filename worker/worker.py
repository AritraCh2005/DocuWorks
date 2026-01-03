import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Create Celery app
celery_app = Celery('worker', broker=os.getenv("CELERY_BROKER_URL"))

# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# Import all task modules to register them
from .image import compress as img_compress
from .image import convert as img_convert
from .image import resize as img_resize
from .pdf import compress as pdf_compress
from .pdf import merge as pdf_merge
from .pdf import extract as pdf_extract

# Print registered tasks for debugging
print("Registered tasks:", list(celery_app.tasks.keys()))

if __name__ == '__main__':
    celery_app.start()