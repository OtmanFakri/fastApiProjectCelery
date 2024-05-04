from celery import Celery

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