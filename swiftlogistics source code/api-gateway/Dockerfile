FROM python:3.11-slim

WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY api-gateway/ /app/
COPY client-portal/ /app/static/client-portal/
COPY driver-app/ /app/static/driver-app/

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "app.py"]
