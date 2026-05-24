FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/ros-adapter/ /app/

EXPOSE 5003

CMD ["python", "app.py"]
