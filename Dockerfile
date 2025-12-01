FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY pyproject.toml poetry.lock* ./

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

COPY /app .

CMD ["python", "main.py"]
