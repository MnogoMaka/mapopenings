FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY db_utils.py .
RUN mkdir -p /app/assets /app/screenshots

EXPOSE 8050

CMD ["python", "app.py"]
