FROM apache/airflow:slim-2.10.4-python3.12

ENV AIRFLOW_HOME=/opt/airflow

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

WORKDIR ${AIRFLOW_HOME}
USER ${AIRFLOW_UID}