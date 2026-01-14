FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY minidb/ ./minidb/
COPY demo_app/ ./demo_app/

# Create data directory
RUN mkdir -p /app/demo_app/expense_data

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV FLASK_ENV=production

# Expose port
EXPOSE 8080

# Run the application
WORKDIR /app/demo_app
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "app:app"]
