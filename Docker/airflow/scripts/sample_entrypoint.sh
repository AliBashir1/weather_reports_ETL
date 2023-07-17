#!/usr/bin/env bash
if [ -f /project_requirements.txt ]
then
      pip install -r /project_requirements.txt
fi
case "$1" in
  webserver)
    airflow db init
    airflow users create -u airflow -p airflow -r Admin -e airflow@airflow.com -f apache -l airflow
    exec airflow webserver
    ;;
scheduler)
  sleep 30
    exec airflow "$@"
    ;;
  flower)
    sleep 30
    exec airflow celery "$@"
    ;;
  worker)
    sleep 30
    exec airflow celery "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac