FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml poetry.lock /app/

RUN pip install poetry && \
    poetry install --no-dev --no-interaction --no-ansi

COPY src/arbirich /app/src/arbirich

COPY main.py /app/

EXPOSE 8000

ENV PYTHONUNBUFFERED=1

CMD ["poetry", "run", "python", "main.py"]
