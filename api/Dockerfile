FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY api/requirements.txt /tmp/api-requirements.txt
COPY shared/requirements.txt /tmp/shared-requirements.txt
RUN pip install --no-cache-dir -r /tmp/shared-requirements.txt -r /tmp/api-requirements.txt
COPY shared/ /app/shared/
COPY api/ /app/api/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--loop", "uvloop"]
