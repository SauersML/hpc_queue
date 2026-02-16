FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app/run.py /app/run.py
RUN chmod +x /app/run.py

ENTRYPOINT ["python", "/app/run.py"]
