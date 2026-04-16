FROM python:3.11-slim

# System deps for OCR and image processing.
# Note: python:3.11-slim is Debian Bookworm — libgl1-mesa-glx was renamed to libgl1.
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p transporters output

EXPOSE 8080

CMD gunicorn "src.web.app:app" \
    --bind "0.0.0.0:${PORT:-8080}" \
    --threads 4 \
    --timeout 300 \
    --worker-class sync \
    --log-level info
