FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Set environment variables
ENV PYTHONBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# Copy requirements first (for better caching)
COPY requirements.txt .

# INstall Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
Copy . .

# Create data directory for persistence
RUN mkdir -p /app/data

# Expose port for FastAPI
EXPOSE 8000

# Default command to run the app
CMD ["python", "-m", "rdbms.repl"]
