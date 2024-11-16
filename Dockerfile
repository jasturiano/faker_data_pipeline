FROM python:3.11-slim 

WORKDIR /app

# Install make
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry

# Copy the pyproject.toml and poetry.lock files
COPY pyproject.toml poetry.lock ./

# Copy the application code
COPY . .

# Install dependencies
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi


# Set environment variables
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Command to run the pipeline using make
ENTRYPOINT ["make", "pipeline"]
