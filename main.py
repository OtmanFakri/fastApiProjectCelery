import asyncio
import base64
from datetime import datetime
import os
from enum import Enum
from io import BytesIO

import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends,WebSocket
from celery_worker import add, save_file_task
from pydantic import BaseModel
from sqlalchemy.orm import Session

from config import Base, engine, get_db, init_models
from models.files_upload import files_upload


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



app = FastAPI()

os.makedirs("files", exist_ok=True)

@app.on_event("startup")
async def startup_event():
    await init_models()


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

@app.post("/save_file/")
async def save_file(file: UploadFile = File(...)):
    file_path = os.path.join("files", file.filename)
    try:
        contents = await file.read()
        result = save_file_task.delay(file_path, contents)
        return {"message": f"File {file.filename} is being processed. with id {result.id} "}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"There was an error uploading the file: {str(e)}")
    finally:
        await file.close()

@app.get("/files/{file_id}")
def get_file_by_id(file_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a file by its ID.
    """
    file_record = db.query(files_upload).filter(files_upload.id == file_id).first()
    if file_record:
        return file_record
    else:
        raise HTTPException(status_code=404, detail="File not found")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        data = await websocket.receive_text()
        # Decode the base64 file
        file_content = base64.b64decode(data)
        # Read the file using Pandas
        df = pd.read_excel(BytesIO(file_content))
        # Send back the first 10 rows as JSON
        await websocket.send_json(df.head(10).to_dict(orient='records'))
    except Exception as e:
        await websocket.send_text(f"Error: {str(e)}")
        await websocket.close()