FROM python:3.9-slim

WORKDIR /src/app

COPY requirements/requirements.txt .
COPY app /src/app


RUN apt-get update

RUN pip install --upgrade pip

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8081

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]