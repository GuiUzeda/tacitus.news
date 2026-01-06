FROM python:3.11-slim

WORKDIR /code

ENV PYTHONPATH=/code

COPY services/backend/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir -r /code/requirements.txt

COPY common /code/common

RUN pip install --no-cache-dir /code/common

COPY services/backend/app /code/app
