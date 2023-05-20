FROM apache/airflow:2.6.0
USER root

USER root

RUN apt-get update && \
  apt-get install -y --no-install-recommends vim && \
  apt-get autoremove -yqq --purge && \
  apt-get clean  && \
  apt-get update && \
  apt-get install -y python3-pip

USER airflow

RUN pip3 install google-api-python-client && \
    pip3 install apache-airflow-providers-amazon && \
    pip3 install yandexcloud && \
    pip3 install apache-airflow-providers-yandex
