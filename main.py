import os
from enum import Enum
from io import BytesIO
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from celery_worker import add
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker, Session

engine = create_engine('sqlite:///sales.db', echo = True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Customers(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    email = Column(String)

class files_upload(Base):
    __tablename__ = 'files_upload'

    id = Column(Integer, primary_key=True)
    file_url = Column(String)
    status = Column(String)

class files_uploadSchma(BaseModel):
    file_url: str
    status:str

class Customer(BaseModel):
    id: int
    name: str
    address: str
    email: str

class UploadStatus(Enum):
    UPLOADING_PENDING = "uploading_Pending"
    UPLOADING_SUCCESS = "uploading_Success"
    UPLOADING_FAILED = "uploading_Failed"
    INSERTING_FAILED = "inserting_Failed"
    INSERTING_SUCCESS = "inserting_Success"
    INSERTING_PENDING = "inserting_Pending"


class FilesUpload(BaseModel):
    file_url: str
    status: UploadStatus


Base.metadata.create_all(engine)

app = FastAPI()

os.makedirs("files", exist_ok=True)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "Hello "}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

@app.get("/process")
async def process_endpoint(a:int , b:int):
    result = add.delay(a, b)
    return { "task_id": result.id }


@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    if file.filename.endswith('.xlsx'):
        # Read the contents of the file
        contents = await file.read()
        data = pd.read_excel(BytesIO(contents))
        # Convert DataFrame to list of Customer models
        customers = data.to_dict(orient='records')  # Convert DataFrame to list of dicts
        return [Customer(**customer) for customer in customers]
    else:
        return {"error": "Unsupported file format"}


@app.post("/save_file/")
async def save_file(file: UploadFile = File(...), db: Session = Depends(get_db)):
    file_path = os.path.join("files", file.filename)
    try:
        contents = await file.read()
        with open(file_path, 'wb+') as f:
            f.write(contents)
        new_file = files_upload(file_url=file_path, status="Pending")
        db.add(new_file)
        db.commit()
        db.refresh(new_file)
        return {"message": f"Successfully uploaded {file.filename} with Status: {new_file.status}"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"There was an error uploading the file: {str(e)}")
    finally:
        await file.close()