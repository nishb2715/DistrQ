FROM python:3.12-slim
WORKDIR /app
COPY watchdog/requirements.txt /tmp/watchdog-requirements.txt
COPY shared/requirements.txt /tmp/shared-requirements.txt
RUN pip install --no-cache-dir -r /tmp/shared-requirements.txt -r /tmp/watchdog-requirements.txt
COPY shared/ /app/shared/
COPY watchdog/ /app/watchdog/
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "watchdog.watchdog"]
