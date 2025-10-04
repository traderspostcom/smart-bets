# src/app/main.py
from __future__ import annotations

from fastapi import FastAPI

from .live import router as live_router
from .admin import admin_router

app = FastAPI(title="Smart Bets")

# Public + admin routers
app.include_router(live_router)
app.include_router(admin_router)

@app.get("/health")
def health():
    return {"ok": True}
