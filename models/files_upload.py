from sqlalchemy import Column, Integer, String

from config import Base


class files_upload(Base):
    __tablename__ = 'files_upload'

    id = Column(Integer, primary_key=True)
    file_url = Column(String)
    status = Column(String)