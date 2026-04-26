FROM python:3.12-slim
WORKDIR /app
COPY worker/requirements.txt /tmp/worker-requirements.txt
COPY shared/requirements.txt /tmp/shared-requirements.txt
RUN pip install --no-cache-dir -r /tmp/shared-requirements.txt -r /tmp/worker-requirements.txt
COPY shared/ /app/shared/
COPY worker/ /app/worker/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "worker.worker"]
