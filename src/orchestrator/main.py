"""Main FastAPI application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from orchestrator.routes import job, workflow
from orchestrator.settings import settings
from orchestrator import workflow_registry


def _configure_logging() -> None:
    logging.basicConfig(level=logging.CRITICAL)

    for logger_name in ("uvicorn", "uvicorn.access", "fastapi"):
        framework_logger = logging.getLogger(logger_name)
        framework_logger.handlers.clear()
        framework_logger.propagate = False
        framework_logger.disabled = True

    # Keep uvicorn.error enabled so unhandled exceptions are visible
    uvicorn_error = logging.getLogger("uvicorn.error")
    uvicorn_error.handlers.clear()
    uvicorn_error.propagate = False
    uvicorn_error.setLevel(logging.WARNING)

    app_logger = logging.getLogger("orchestrator")
    app_logger.disabled = False
    app_logger.setLevel(logging.INFO)
    app_logger.propagate = False
    if not app_logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s"))
        app_logger.addHandler(handler)


_configure_logging()
logger = logging.getLogger("orchestrator")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan."""
    # FastAPI/Uvicorn CLI can reconfigure logging after module import.
    # Re-apply suppression at startup so access logs stay disabled.
    _configure_logging()

    if settings.workflow_settings_file:
        try:
            await workflow_registry.load_from_file(settings.workflow_settings_file)
        except (FileNotFoundError, ValidationError, OSError) as exc:
            raise RuntimeError(f"Failed to load workflow settings from {settings.workflow_settings_file}") from exc
    yield


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Omotes REST API",
        description="REST API for Omotes job management and workflows",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(job.router)
    app.include_router(workflow.router)

    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        return {"status": "ok"}

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "orchestrator.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=True,
        log_level=settings.log_level,
    )
