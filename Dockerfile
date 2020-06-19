FROM python:3.7
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . /usr/src/app
ENV AIRFLOW__CORE__DAGS_FOLDER /usr/src/app/airflow_dags
ENV GOOGLE_APPLICATION_CREDENTIALS /usr/src/app/googlecredentials.json
ENV SCRAPY_PATH /usr/src/app/scrapy_sofifa/
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT 'google-cloud-platform://?extra__google_cloud_platform__project=fifaengineering&extra__google_cloud_platform__key_path=/usr/src/app/googlecredentials.json'
RUN chmod 755 /usr/src/app/entrypoint.sh
ENTRYPOINT [ "/usr/src/app/entrypoint.sh" ]