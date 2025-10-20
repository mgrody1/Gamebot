# syntax=docker/dockerfile:1

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive

# Create non-root user for VS Code
RUN apt-get update && \
    apt-get install -y --no-install-recommends git graphviz build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -ms /bin/bash vscode

WORKDIR /workspace

COPY Pipfile Pipfile.lock /workspace/

RUN pip install --upgrade pip pipenv && \
    chown -R vscode:vscode /workspace

USER vscode
