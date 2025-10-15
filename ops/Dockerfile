FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY alembic.ini /app/alembic.ini
COPY app /app
EXPOSE 8000
CMD ["sh","-c","alembic upgrade head && uvicorn main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 5"]
