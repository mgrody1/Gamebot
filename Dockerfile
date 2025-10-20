FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY Pipfile Pipfile.lock /app/

RUN pip install --upgrade pip pipenv && \
    pipenv install --system --deploy

COPY . /app

ENV PYTHONPATH=/app

CMD ["python", "-m", "Database.load_survivor_data"]
