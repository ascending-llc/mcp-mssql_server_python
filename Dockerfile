FROM python:3.12-slim

# Set basic environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies and Microsoft ODBC Driver for SQL Server
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        unixodbc \
        unixodbc-dev \
        libpq-dev \
        libgl1 \
        libglib2.0-0 \
        curl \
        gnupg \
        netcat-traditional \
        wget \
        ca-certificates \
    # Install Microsoft ODBC driver using modern GPG key method
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Verify ODBC driver installation
RUN odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

# Create app user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy dependency files and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Change ownership to app user
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Environment variables for configuration
ENV FASTMCP_TRANSPORT=http \
    FASTMCP_HOST=0.0.0.0 \
    FASTMCP_PORT=3333 \
    FASTMCP_LOG_LEVEL=INFO \
    CACHE_ENABLED=true \
    ENABLE_ASYNC=true \
    ENABLE_DYNAMIC_RESOURCES=true \
    MAX_ROWS_LIMIT=500 \
    DB_POOL_MIN_SIZE=2 \
    DB_POOL_MAX_SIZE=10

# Expose port
EXPOSE 3333

# Start the server
CMD ["python", "-m", "mssql_mcp_server.main"]
