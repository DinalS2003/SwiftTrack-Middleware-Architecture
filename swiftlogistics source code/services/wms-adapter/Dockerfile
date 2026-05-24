FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/wms-adapter/ /app/

EXPOSE 5004
EXPOSE 9999

CMD ["python", "app.py"]
