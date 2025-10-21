FROM apache/airflow:2.9.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:0 requirements.txt /tmp/airflow-requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt && rm /tmp/airflow-requirements.txt
