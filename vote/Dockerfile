FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 80

# Run with single process and multi-threaded mode so the background auto-voter thread runs exactly once
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:80", "--workers", "1", "--threads", "4", "--access-logfile", "-", "--error-logfile", "-"]
