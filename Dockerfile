FROM python:3.7
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . /usr/src/app
ENV AIRFLOW__CORE__DAGS_FOLDER /usr/src/app/airflow_dags
ENV GOOGLE_APPLICATION_CREDENTIALS /usr/src/app/googlecredentials.json
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
        apt-get install apt-transport-https ca-certificates gnupg && \
        curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
        apt-get update && \
        apt-get install google-cloud-sdk -y && \
        gcloud auth activate-service-account dataloader@fifaeng.iam.gserviceaccount.com --key-file=/usr/src/app/googlecredentials.json --project=fifaeng
ENV SCRAPY_PATH /usr/src/app/scrapy_sofifa/
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT 'google-cloud-platform://?extra__google_cloud_platform__project=fifaeng&extra__google_cloud_platform__key_path=/usr/src/app/googlecredentials.json'
RUN chmod 755 /usr/src/app/entrypoint.sh
ENTRYPOINT [ "/usr/src/app/entrypoint.sh" ]