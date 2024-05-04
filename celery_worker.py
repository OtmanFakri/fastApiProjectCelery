from celery import Celery
from fastapi import HTTPException

from main import files_upload

celery = Celery(
    'celery_worker',
    broker='redis://redis/0',
    backend='redis://redis/0'
)

@celery.task(name='celery_worker.add')  # Explicitly name the task
def add(a, b):
    for i in range(a,b):
        print(i)
    return {"number":a+b}


@celery.task(bind=True)
def save_file_task(file_path, file_content, db_session):
    try:
        with open(file_path, 'wb+') as f:
            f.write(file_content)
        new_file = files_upload(file_url=file_path, status="Pending")
        db_session.add(new_file)
        db_session.commit()
        db_session.refresh(new_file)
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"There was an error uploading the file: {str(e)}")