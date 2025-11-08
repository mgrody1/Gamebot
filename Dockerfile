FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        build-essential \
        graphviz \
        graphviz-dev \
        pkg-config \
        python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -ms /bin/bash vscode

COPY --chown=vscode:vscode Pipfile Pipfile.lock /app/

RUN pip install --upgrade pip pipenv && \
    pipenv install --system --deploy

COPY --chown=vscode:vscode . /app

USER vscode

ENV PYTHONPATH=/app

CMD ["python", "-m", "Database.load_survivor_data"]
