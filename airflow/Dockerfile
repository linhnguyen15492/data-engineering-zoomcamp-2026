FROM apache/airflow:slim-3.1.7-python3.12

ENV AIRFLOW_HOME=/opt/airflow

WORKDIR $AIRFLOW_HOME

USER root
RUN apt-get update -qq && apt-get install -y vim -qqq

COPY ./scripts/requirements.txt .
USER airflow
RUN pip install -r requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

USER root

# Install prerequisites
RUN sudo apt-get update -y \
    && sudo apt-get install ca-certificates gnupg curl

# Add the Google Cloud SDK repository and install the SDK
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
     && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
     && apt-get update -y \
     && apt-get install google-cloud-cli -y
    
COPY scripts scripts
RUN chmod +x scripts/*.sh

USER $AIRFLOW_UID