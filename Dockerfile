FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Poetry
RUN pip install poetry

# Copy project files
COPY pyproject.toml poetry.lock* ./
COPY src/ ./src/

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Copy remaining files
COPY config.yaml ./
COPY examples/ ./examples/

# Set environment variables
ENV PYTHONPATH=/app

# Run the scraper
ENTRYPOINT ["python", "-m", "tic_mrf_scraper"]
CMD ["--config", "config.yaml", "--output", "output"] 