FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY ops/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt && rm -f ./requirements.txt

COPY ops /app/ops
RUN chmod +x /app/ops/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/ops/entrypoint.sh"]
