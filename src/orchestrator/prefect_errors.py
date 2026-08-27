"""Shared helpers for translating Prefect-related runtime failures."""

import re

from fastapi import HTTPException

_PREFECT_UNAVAILABLE_PREFIX = "Prefect server is unavailable at "
_DEPLOYMENT_NOT_FOUND_PATTERN = re.compile(
    r"Prefect deployment '(?P<deployment>[^']+)' not found for run '(?P<run>[^']+)'"
)
_URL_PATTERN = re.compile(r"https?://[^\s.,;]+")


def raise_for_prefect_runtime_error(exc: RuntimeError) -> None:
    """Translate common Prefect runtime failures into HTTP responses."""
    message = str(exc).strip()

    deployment_match = _DEPLOYMENT_NOT_FOUND_PATTERN.search(message)
    if deployment_match is not None:
        deployment = deployment_match.group("deployment")
        run_name = deployment_match.group("run")
        raise HTTPException(
            status_code=404,
            detail=(
                f"Flow deployment '{deployment}' is not available in Prefect for run '{run_name}'. "
                "Check the workflow type/version and available Prefect deployments."
            ),
        ) from exc

    if message.startswith(_PREFECT_UNAVAILABLE_PREFIX):
        match = _URL_PATTERN.search(message)
        if match is None:
            raise HTTPException(status_code=503, detail=message) from exc

        raise HTTPException(
            status_code=503,
            detail=message,
        ) from exc
