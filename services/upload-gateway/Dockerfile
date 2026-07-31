# AkiDB Upload Gateway Service
# Multi-stage build for macOS-hosted development and portable containers

FROM python:3.12-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy source files for build
COPY pyproject.toml README.md ./
COPY gateway/ ./gateway/
COPY main.py .

# Install Python dependencies
RUN pip install --no-cache-dir .

# Final stage
FROM python:3.12-slim

WORKDIR /app

# Copy installed packages
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application
COPY gateway/ ./gateway/
COPY main.py .

# Create non-root user
RUN useradd -m -u 1000 gateway && chown -R gateway:gateway /app
USER gateway

# Expose port
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8081/health')"

# Run
ENTRYPOINT ["python", "main.py"]
