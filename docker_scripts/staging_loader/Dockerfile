FROM python:3.12-slim

# Send logs straight to stdout/stderr so Airflow shows them live in the task log.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy requirements first so the dependency layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY sql/ sql/
COPY seeds/ seeds/

# Do not run as root.
RUN useradd --create-home appuser
USER appuser

# ENTRYPOINT is the program, the DAG's `command=[...]` supplies its arguments.
ENTRYPOINT ["python", "main.py"]
