FROM python:3.12.11-alpine3.22
WORKDIR /opt/init
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY init_buckets.py .
USER 10001:10001
ENTRYPOINT ["python", "/opt/init/init_buckets.py"]
