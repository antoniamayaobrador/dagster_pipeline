# Use the official Python image as a base image with a specific version to avoid vulnerabilities
FROM python:3.10-slim@sha256:6b5e6e8e6f9b4c9a3d3b7e9b5c5e5d5e5d5e5d5e5d5e5d5e5d5e5d5e5d5e5d5e5d5

# Update package lists and upgrade installed packages for security
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/opt/dagster/app/weather/weather_project \
    DBT_PROJECT_DIR=/opt/dagster/app/weather/weather_project

# Set working directory
WORKDIR /opt/dagster/app

# Create a non-root user to run the application
RUN useradd -m appuser && \
    chown -R appuser:appuser /opt/dagster/app

# Switch to non-root user
USER appuser

# Set the working directory
WORKDIR /opt/dagster/app

# Copy requirements first to leverage Docker cache
COPY --chown=appuser:appuser requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create the dbt project directory with the correct permissions
RUN mkdir -p /opt/dagster/app/weather/weather_project && \
    chown -R appuser:appuser /opt/dagster/app/weather/weather_project

# Copy the rest of the application with correct permissions
COPY --chown=appuser:appuser . .

# Ensure necessary directories exist with correct permissions
RUN mkdir -p /opt/dagster/app/weather/weather_project/target && \
    chown -R appuser:appuser /opt/dagster/app/weather/weather_project/target

# Set the working directory to the dbt project directory when running dbt commands
WORKDIR /opt/dagster/app/weather/weather_project

# The default command to run when starting the container
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "weather/definitions.py"]
