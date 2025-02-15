ARG PYTHON_VER=3.12
ARG POETRY_VER=1.8.4

FROM python:${PYTHON_VER}-slim AS base

ARG POETRY_VER

RUN pip install \
        --no-cache-dir \
        --upgrade \
      pip && \
    pip install \
        --no-cache-dir \
      "poetry==${POETRY_VER}"

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN poetry install --without dev --no-interaction --no-root --all-extras

COPY . .

CMD ["poetry", "run", "--no-interaction", "python", "main.py"]
