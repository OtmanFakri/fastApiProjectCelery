from sqlalchemy.orm import Session

from models.files_upload import files_upload


async def file_save(file_path, file_content):
    try:
        with open(file_path, 'wb+') as f:
            f.write(file_content)
        new_file = files_upload(file_url=file_path, status="uploading_pending")

    except Exception as e:
        await db.rollback()
        print(f"There was an error uploading the file: {str(e)}")
        raise e