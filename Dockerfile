FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY data/ ./data/

# Create a script to convert SQL files to SQLite databases
COPY create_databases.py .

# Ensure we have the directory structure
RUN mkdir -p /app/data

# Convert SQL files to SQLite databases if they exist
RUN python create_databases.py

# Set environment variables
ENV PYTHONPATH=/app

# Run the FastAPI application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]