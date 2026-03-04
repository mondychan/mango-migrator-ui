FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app.py /app/app.py
COPY index.html /app/index.html
COPY sync-logo.svg /app/sync-logo.svg

RUN mkdir -p /app/data/uploads /app/data/reports

EXPOSE 8099
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]
