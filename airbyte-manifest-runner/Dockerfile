FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0

WORKDIR /app
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

FROM python:3.12-slim-bookworm

# Create a non-root user and group
RUN groupadd --gid 1000 airbyte && \
    useradd --uid 1000 --gid airbyte --shell /bin/bash --create-home airbyte && \
    chown -R airbyte:airbyte /usr/local/share/ca-certificates && \
    chown -R airbyte:airbyte /etc/ssl/certs

COPY --from=builder --chown=airbyte:airbyte /app /app

# Run app as non-root user
USER airbyte:airbyte

# Set working directory to the project root
WORKDIR /app

# Set PATH to include the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]