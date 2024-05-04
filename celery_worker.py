import pandas as pd
from celery import Celery, shared_task
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import SessionLocal
from models.Customers import Customers
from models.files_upload import files_upload
engine_celery = create_engine('sqlite:///sales.db', echo = True)
SessionLocal_celery = sessionmaker(autocommit=False, autoflush=False, bind=engine_celery)

celery = Celery(
    'celery_worker',
    broker='redis://redis/0',
    backend='redis://redis/0'
)



def update_file_status(file_path, status):
    session = SessionLocal_celery()
    file_record = session.query(files_upload).filter_by(file_url=file_path).first()
    if file_record:
        file_record.status = status
        session.commit()

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
        # Chain the task to process the customers after saving the file
        process_customers_task.delay(file_path)
        return {"file_path": file_path, "status": "uploading_Pending"}
    except Exception as e:
        print(f"There was an error uploading the file: {str(e)}")
        raise e

@celery.task(name='celery_worker.process_customers_task')
def process_customers_task(file_path):
    session = SessionLocal_celery()  # Get a new SQLAlchemy session
    try:
        # Read the Excel file using Pandas
        df = pd.read_excel(file_path)
        # Process each row and create a new Customer object
        for index, row in df.iterrows():
            new_customer = Customers(
                name=row['name'],
                address=row['address'],
                email=row['email']
            )
            session.add(new_customer)
        session.commit()
        update_file_status(file_path, "Completed")
        return {"message": "Data processed successfully", "status": "Completed"}
    except Exception as e:
        print(f"Failed to process data: {str(e)}")
        update_file_status(file_path, "Failed")
        raise


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
