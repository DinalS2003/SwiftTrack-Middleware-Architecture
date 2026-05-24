FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/order-service/ /app/

EXPOSE 5001

CMD ["python", "app.py"]
