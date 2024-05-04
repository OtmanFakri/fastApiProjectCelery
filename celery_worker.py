from celery import Celery, shared_task
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import SessionLocal
from models.files_upload import files_upload
engine_celery = create_engine('sqlite:///sales.db', echo = True)
SessionLocal_celery = sessionmaker(autocommit=False, autoflush=False, bind=engine_celery)

celery = Celery(
    'celery_worker',
    broker='redis://redis/0',
    backend='redis://redis/0'
)





@celery.task(name='celery_worker.save_file_task')
def save_file_task(file_path, file_content):
    session = SessionLocal_celery()
    try:
        with open(file_path, 'wb+') as f:
            f.write(file_content)
        new_file = files_upload(file_url=file_path, status="uploading_Pending")
        session.add(new_file)
        session.commit()
        session.refresh(new_file)
        return {"file_path": file_path, "status": "uploading_Pending"}
    except Exception as e:
        print(f"There was an error uploading the file: {str(e)}")
        raise e


@celery.task(name='celery_worker.add')  # Explicitly name the task
def add(a, b):
    for i in range(a, b):
        print(i)
    return {"number": a+b}


"""
    db_session = SessionLocal()
    try:
        with open(file_path, 'wb+') as f:
            f.write(file_content)
        new_file = files_upload(file_url=file_path, status="uploading_Pending")
        db_session.add(new_file)
        db_session.commit()
        db_session.refresh(new_file)
    except Exception as e:
        db_session.rollback()
        print(f"There was an error uploading the file: {str(e)}")
    finally:
        db_session.close()
        """
