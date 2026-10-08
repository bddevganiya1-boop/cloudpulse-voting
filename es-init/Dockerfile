FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir elasticsearch==8.13.1
COPY create_indices.py .

CMD ["python", "create_indices.py"]
