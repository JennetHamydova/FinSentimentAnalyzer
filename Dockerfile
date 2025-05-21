# === Dockerfile projektu FinSentimentAnalyzer ===
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
# logi natychmiast w konsolio
