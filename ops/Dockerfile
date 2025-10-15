FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY ops/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

COPY . /app
COPY ops/alembic.ini /app/ops/alembic.ini
RUN chmod +x /app/ops/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/ops/entrypoint.sh"]
