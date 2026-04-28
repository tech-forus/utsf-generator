FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-hin \
    poppler-utils \
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

RUN mkdir -p transporters output && chmod +x /app/start.sh

# Build-time smoke test — import error here fails the build with a visible reason
RUN PYTHONPATH=/app/src python -c "import sys; sys.path.insert(0, '/app/src'); import os; os.chdir('/app/src/web'); from app import app; print('[SMOKE TEST OK]')"

EXPOSE 8080

CMD ["python", "/app/start.py"]
