FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY getNO2Readings.py db.py quality.py publisher.py scheduler.py redis_ping.py ingest_air.py ./
EXPOSE 8000
CMD ["python", "ingest_air.py"]
