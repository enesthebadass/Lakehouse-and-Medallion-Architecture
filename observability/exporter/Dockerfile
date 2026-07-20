FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

COPY exporter.py .

USER 65532:65532

EXPOSE 9108

CMD ["python", "exporter.py"]
