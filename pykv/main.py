# ============================ main.py ============================
# Full PyKV server:
# - UI (login/register/dashboard/stats)
# - Session login
# - Key operations: set/get/delete/keys/clear
# - WAL persistence
# - Replication foundation
# ================================================================

import os
import requests

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from fastapi.templating import Jinja2Templates

from store import PyKVStore
from persistence import WAL
from auth import register_user, validate_user


ROLE = os.getenv("ROLE", "primary")
SECONDARY_URL = os.getenv("SECONDARY_URL")

app = FastAPI(title="PyKV", version="2.0")

# session middleware
app.add_middleware(SessionMiddleware, secret_key="pykv-secret-key")

# templates + static
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# store + WAL
store = PyKVStore(capacity=100)
wal = WAL("data/pykv.log")
wal.recover(store)


def require_login(request: Request):
    if not request.session.get("user"):
        raise HTTPException(status_code=401, detail="Login required")


# -------------------- UI ROUTES --------------------

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)

    return templates.TemplateResponse("dashboard.html", {"request": request, "role": ROLE})


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if not validate_user(username, password):
        return templates.TemplateResponse("login.html", {"request": request, "error": "Invalid email or password"})

    request.session["user"] = username
    return RedirectResponse("/", status_code=302)


@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@app.post("/register")
def register(request: Request, username: str = Form(...), password: str = Form(...)):
    ok, msg = register_user(username, password)

    if not ok:
        return templates.TemplateResponse("register.html", {"request": request, "error": msg})

    return RedirectResponse("/login", status_code=302)


@app.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/stats-ui", response_class=HTMLResponse)
def stats_ui(request: Request):
    require_login(request)
    return templates.TemplateResponse("stats.html", {"request": request})


# -------------------- API ROUTES --------------------

@app.post("/set")
def api_set(request: Request, payload: dict):
    require_login(request)

    key = payload.get("key")
    value = payload.get("value")
    ttl = payload.get("ttl")

    if not key:
        raise HTTPException(status_code=400, detail="key is required")

    store.set(key, value, ttl=ttl)
    wal.append_set(key, value, ttl)

    # replication
    if ROLE == "primary" and SECONDARY_URL:
        try:
            requests.post(f"{SECONDARY_URL}/replica/set", json={"key": key, "value": value, "ttl": ttl}, timeout=2)
        except Exception:
            pass

    return {"message": "SET ok", "key": key}


@app.get("/get/{key}")
def api_get(request: Request, key: str):
    require_login(request)

    value = store.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")

    return {"key": key, "value": value, "ttl_remaining": store.ttl_remaining(key)}


@app.delete("/delete/{key}")
def api_delete(request: Request, key: str):
    require_login(request)

    ok = store.delete(key)

    if not ok:
        raise HTTPException(status_code=404, detail="Key not found")

    wal.append_delete(key)

    # replication
    if ROLE == "primary" and SECONDARY_URL:
        try:
            requests.delete(f"{SECONDARY_URL}/replica/delete/{key}", timeout=2)
        except Exception:
            pass

    return {"message": "DELETE ok", "key": key}


@app.get("/keys")
def api_keys(request: Request):
    """
    Returns all keys in store (for frontend display)
    """
    require_login(request)
    return {"keys": store.keys(), "count": len(store.keys())}


@app.delete("/clear")
def api_clear(request: Request):
    """
    Deletes all keys from store (frontend clear button)
    """
    require_login(request)

    # delete all keys one by one so LRU stays consistent
    for k in store.keys():
        store.delete(k)
        wal.append_delete(k)

    return {"message": "All keys cleared"}


@app.get("/stats")
def api_stats(request: Request):
    require_login(request)
    return store.stats(wal_size=wal.size())


@app.post("/compact")
def compact_wal(request: Request):
    require_login(request)
    wal.compact(store)
    return {"message": "WAL compacted successfully"}


# -------------------- REPLICATION ENDPOINTS --------------------

@app.post("/replica/set")
def replica_set(payload: dict):
    if ROLE != "secondary":
        return JSONResponse({"error": "Not a secondary node"}, status_code=400)

    key = payload.get("key")
    value = payload.get("value")
    ttl = payload.get("ttl")

    store.set(key, value, ttl=ttl)
    wal.append_set(key, value, ttl)

    return {"message": "replicated set ok"}


@app.delete("/replica/delete/{key}")
def replica_delete(key: str):
    if ROLE != "secondary":
        return JSONResponse({"error": "Not a secondary node"}, status_code=400)

    store.delete(key)
    wal.append_delete(key)

    return {"message": "replicated delete ok"}
