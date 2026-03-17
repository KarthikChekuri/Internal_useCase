FROM python:3.12-slim AS builder

# Install Poetry
RUN pip install poetry

WORKDIR /app

# Copy dependency files first for layer caching
COPY pyproject.toml poetry.lock* ./

# Install dependencies only (skip the project itself — source not copied yet)
RUN poetry config virtualenvs.create false \
    && poetry install --without dev --no-interaction --no-ansi --no-root

# Copy the rest of the project
COPY . .

# Install the project itself (breach-search script entry point)
RUN poetry install --without dev --no-interaction --no-ansi

ENTRYPOINT ["breach-search"]
