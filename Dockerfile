FROM python:3.6-alpine
RUN apk add --no-cache postgresql-dev gcc python3-dev musl-dev
ADD requirements.txt /code/requirements.txt
WORKDIR /code
RUN pip install -r requirements.txt
CMD ["python", "worker.py"]
