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

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

COPY --chown=vscode:vscode pyproject.toml uv.lock /app/

RUN uv sync --locked --no-install-project --no-default-groups --group pipeline

COPY --chown=vscode:vscode . /app

USER vscode

ENV PYTHONPATH=/app

CMD ["python", "-m", "Database.load_survivor_data"]
