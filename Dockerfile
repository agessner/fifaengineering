FROM python:3.7
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN chmod 755 /app/entrypoint.sh
ENTRYPOINT [ "/app/entrypoint.sh" ]