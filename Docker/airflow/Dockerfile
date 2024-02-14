FROM python:3.8.9
LABEL maintainer="Ali Bashir"

ARG AIRFLOW_VERSION=2.5.2
ARG AIRFLOW_USER_HOME=/project_files

# set airflow_home
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}
ENV PYTHONPATH="${PYTHONPATH}:/project_files"

COPY airflow-requirements.txt /airflow-requirements.txt

RUN apt-get update -yqq \
    && apt-get upgrade  -yqq \
    && apt-get install -yqq --no-install-recommends \
    && apt-get install libpq-dev \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install --upgrade "pip==21.1.1" \
    && pip install -U pip setuptools wheel \
    && pip install apache-airflow[crypto,postgres,jdbc,celery,http,amazon]==${AIRFLOW_VERSION} --constraint /airflow-requirements.txt \
    && pip install 'redis' \
    && pip install 'psycopg2' \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/*



COPY scripts/entrypoint.sh entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg


RUN chmod +x /entrypoint.sh
RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
USER airflow

#CMD ["webserver"]
