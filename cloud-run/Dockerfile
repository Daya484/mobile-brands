# Cloud Run Extractor — Dockerfile
# =====================================
# Single-stage build — simple & reliable for Cloud Run
# Base: Python 3.11-slim

FROM python:3.11-slim

WORKDIR /app

# Install dependencies globally (no --user, avoids PATH issues)
COPY cloud-run/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy app source
COPY cloud-run/main.py .
COPY cloud-run/gcs_trigger.py .
COPY extract.py .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

EXPOSE 8080

# exec form: uvicorn is in /usr/local/bin, $PORT is set by Cloud Run
CMD exec uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
