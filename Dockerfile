FROM python:3.9-slim

WORKDIR /src/app

COPY requirements/requirements.txt .
COPY app /src/app

# wait for cassandra to be up
COPY scripts/wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

RUN apt-get update && apt-get install -y netcat-openbsd

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8081

CMD ["/wait-for-it.sh", "cassandra:9042", "--timeout=60", "--strict", "--", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]
