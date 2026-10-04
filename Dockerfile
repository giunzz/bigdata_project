FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV SPARK_LOCAL_IP=127.0.0.1
WORKDIR /work
COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /work
CMD ["python", "phuc_analytics.py", "--help"]
