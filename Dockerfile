FROM python:3.11-slim

# Install Java for PySpark 
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

# Copy configuration files
COPY pytest.ini .
COPY setup.cfg .
COPY setup.py .

# Create necessary directories
RUN mkdir -p data/raw data/processed logs

# Make scripts executable
RUN chmod +x scripts/*.sh scripts/*.py 2>/dev/null || true

# Set Python path
ENV PYTHONPATH=/app:${PYTHONPATH}
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "-m", "src.pipeline.main"]