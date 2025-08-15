# Manifest Runner

A service for running low-code Airbyte connectors

## Installation

### 1. Install uv (if not already installed)

```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Install dependencies

```bash
# Install all dependencies from pyproject.toml
uv sync
```

## Running the API

### Development Server

```bash
# Run with uvicorn (recommended for development)
uv run fastapi dev
```

