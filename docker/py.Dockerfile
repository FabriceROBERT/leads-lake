# Image for the Python ingestion tasks (pollers, downloaders), used by Airflow.
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# bake the code in; local dev runs modules from the venv instead
COPY app/ ./app/
COPY ingestion/ ./ingestion/

CMD ["python", "--version"]
