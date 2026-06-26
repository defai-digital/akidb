# AkiDB Document Parser Service
# Multi-stage build for ARM64 (Jetson Thor)

FROM python:3.12-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy source files for build
COPY pyproject.toml README.md ./
COPY parser/ ./parser/
COPY main.py .

# Install Python dependencies
RUN pip install --no-cache-dir .

# Final stage
FROM python:3.12-slim

WORKDIR /app

# Install runtime dependencies for document parsing
RUN apt-get update && apt-get install -y --no-install-recommends \
    libmagic1 \
    poppler-utils \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application
COPY parser/ ./parser/
COPY main.py .

# Create non-root user
RUN useradd -m -u 1000 parser && chown -R parser:parser /app
USER parser

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Run
ENTRYPOINT ["python", "main.py"]
