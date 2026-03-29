"""
FastAPI application for the SCimulator Web UI.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api import scenarios, results, network, data_io
from .services.registry import init_registry


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Set up application state on startup."""
    data_dir = os.environ.get('SCIMULATOR_DATA_DIR', '.')
    app.state.data_dir = Path(data_dir).resolve()
    app.state.registry = init_registry(app.state.data_dir)
    yield
    # Clean up registry connection on shutdown
    app.state.registry.close()


app = FastAPI(
    title="SCimulator",
    description="Distribution Network Simulator",
    version="0.2.0",
    lifespan=lifespan,
)

# CORS for React dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(scenarios.router, prefix="/api")
app.include_router(results.router, prefix="/api")
app.include_router(network.router, prefix="/api")
app.include_router(data_io.router, prefix="/api")
