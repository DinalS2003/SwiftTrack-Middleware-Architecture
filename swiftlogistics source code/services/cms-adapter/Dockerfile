FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/cms-adapter/ /app/

EXPOSE 5002

CMD ["python", "app.py"]
