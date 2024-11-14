# Use an official Python runtime as a parent image
FROM python:3.11-slim as builder

# Set the working directory in the container
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy dependency files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN poetry lock --no-update
RUN poetry config virtualenvs.create false && poetry install --only main --no-interaction --no-ansi

# Copy the application code
COPY . .

# Make scripts executable
RUN chmod +x scripts/run_pipeline.sh

# Create a non-root user for security
RUN useradd -m -u 1000 pipeline_user && chown -R pipeline_user:pipeline_user /app
USER pipeline_user

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Command to run the pipeline
ENTRYPOINT ["./scripts/run_pipeline.sh"]
