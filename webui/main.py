"""Bedrock Invocation Analytics WebUI — Entry point."""

import os
import tomllib
from pathlib import Path

from fastapi import Request
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from nicegui import app, ui

from webui import dashboard  # noqa: F401
from webui import pricing  # noqa: F401

with open(Path(__file__).parent.parent / "pyproject.toml", "rb") as f:
    VERSION = tomllib.load(f)["project"]["version"]

dashboard.VERSION = VERSION

# ── Authentication ──
# Set credentials via environment variables: ADMIN_USER / ADMIN_PASS
# Default: admin / admin (change in production!)
USERS = {os.environ.get("ADMIN_USER", "admin"): os.environ.get("ADMIN_PASS", "admin")}
UNRESTRICTED = {"/login"}


@app.add_middleware
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not app.storage.user.get("authenticated", False):
            if not request.url.path.startswith("/_nicegui") and request.url.path not in UNRESTRICTED:
                return RedirectResponse(f"/login?redirect_to={request.url.path}")
        return await call_next(request)


@ui.page("/login")
def login_page(redirect_to: str = "/") -> RedirectResponse | None:
    if app.storage.user.get("authenticated", False):
        return RedirectResponse("/")

    ui.dark_mode(False)

    def try_login():
        if USERS.get(username.value) == password.value:
            app.storage.user.update({"username": username.value, "authenticated": True})
            ui.navigate.to(redirect_to)
        else:
            ui.notify("Invalid credentials", color="negative")

    with ui.card().classes("absolute-center min-w-[300px]"):
        ui.label("Bedrock Invocation Analytics").classes("text-xl font-bold text-center w-full mb-4")
        username = ui.input("Username").on("keydown.enter", try_login).classes("w-full")
        password = ui.input("Password", password=True, password_toggle_button=True).on("keydown.enter", try_login).classes("w-full")
        ui.button("Log in", on_click=try_login).classes("w-full mt-4")
    return None


ui.run(
    title="Bedrock Invocation Analytics",
    favicon="docs/favicon.svg",
    port=int(os.environ.get("PORT", "8060")),
    reload=False,
    storage_secret=os.environ.get("STORAGE_SECRET", "bedrock-analytics-secret-change-me"),
)
