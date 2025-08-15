from fastapi import FastAPI

from .routers import health, capabilities

app = FastAPI(
    title="Manifest Runner Service",
    description="A service for running low-code Airbyte connectors",
    version="0.1.0",
    contact={
        "name": "Airbyte",
        "url": "https://airbyte.com",
    },
)

app.include_router(health.router)
app.include_router(capabilities.router)
