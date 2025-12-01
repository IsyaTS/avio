"""Lightweight HTTP endpoints for the worker service."""

from fastapi import FastAPI

app = FastAPI(title="avio-worker")


@app.get("/health")
async def health() -> dict[str, bool]:
    """Return basic readiness information for the worker container."""
    return {"ok": True}
