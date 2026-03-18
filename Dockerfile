# BIST Finans Backend — Production Dockerfile
FROM python:3.13-slim

# Sistem bağımlılıkları (tesseract OCR + poppler for PDF processing)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    tesseract-ocr \
    tesseract-ocr-tur \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && tesseract --version

# Çalışma dizini
WORKDIR /app

# requirements önce kopyala (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodunu kopyala
COPY . .

# Port
EXPOSE 8001

# Gunicorn ile başlat — 1 worker (free plan memory icin), 120s timeout
CMD ["gunicorn", "app.main:app", \
     "-w", "1", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8001", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
