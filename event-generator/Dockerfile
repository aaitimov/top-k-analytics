FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /uvx /bin/

COPY pyproject.toml uv.lock /app/

WORKDIR /app

RUN uv sync --locked

COPY . /app/

CMD ["uv", "run", "-m", "app.main"]
