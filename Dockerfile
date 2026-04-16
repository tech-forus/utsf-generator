FROM python:3.11-slim

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

# Build-time smoke test: if this fails the Docker build fails with the
# actual Python error visible in Railway build logs.
RUN cd /app/src/web && PYTHONPATH=/app/src python -c \
    "from app import app; print('[SMOKE TEST OK] Flask app imported cleanly')"

EXPOSE 8080

# Start with Flask's built-in threaded server.
# Simpler than gunicorn, identical behaviour, and errors go straight to stdout
# so Railway logs show the real crash reason.
CMD python -c "
import os, sys
sys.path.insert(0, '/app/src')
os.chdir('/app/src/web')
from app import app
port = int(os.environ.get('PORT', 8080))
print(f'[START] binding on 0.0.0.0:{port}', flush=True)
app.run(host='0.0.0.0', port=port, debug=False, threaded=True, use_reloader=False)
"
