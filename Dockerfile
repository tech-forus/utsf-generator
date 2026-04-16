FROM python:3.11-slim

# System deps:
#   tesseract-ocr     → image/scanned PDF OCR (pytesseract)
#   libgl1 libglib2   → opencv-python-headless
#   libsm6 libxext6   → opencv extras
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache — only rebuilds when requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the full app
COPY . .

# Ensure local dirs exist for standalone / non-volume use
RUN mkdir -p transporters output

EXPOSE 8080

# gunicorn: threaded sync worker handles SSE streams fine.
# Timeout 300s because UTSF generation can take 1-2 min for large files.
# Railway injects $PORT automatically.
CMD gunicorn "src.web.app:app" \
    --bind "0.0.0.0:${PORT:-8080}" \
    --threads 4 \
    --timeout 300 \
    --worker-class sync \
    --log-level info
