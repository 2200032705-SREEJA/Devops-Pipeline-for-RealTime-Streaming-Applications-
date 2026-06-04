FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY spotify_producer.py .

ENTRYPOINT ["python", "spotify_producer.py"]
