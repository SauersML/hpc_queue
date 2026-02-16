FROM python:3.12-slim

WORKDIR /app
COPY app/run.py /app/run.py
RUN chmod +x /app/run.py

ENTRYPOINT ["python", "/app/run.py"]
