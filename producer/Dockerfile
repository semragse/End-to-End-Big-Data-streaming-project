FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir requests confluent-kafka

COPY producer/producer.py .

CMD ["python", "-u", "producer.py"]
