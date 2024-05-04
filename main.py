from fastapi import FastAPI
from celery_worker import add
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String

engine = create_engine('sqlite:///sales.db', echo = True)
Base = declarative_base()

class Customers(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    email = Column(String)

Base.metadata.create_all(engine)

app = FastAPI()



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