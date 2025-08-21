# frontend/main.py
import sys, os
import asyncio
import json
from pathlib import Path

from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Ensure the repo root is on sys.path so that the smart_agents package can be imported
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(repo_root)

# Import the runtime function
from smart_agents.agent_runtime_new import run_task_loop

app = FastAPI()
script_dir = os.path.dirname(__file__)
st_abs_file_path = os.path.join(script_dir, "static/")
app.mount("/static", StaticFiles(directory=st_abs_file_path), name="static")

templates_dir = os.path.dirname(__file__)
templates_abs_file_path = os.path.join(templates_dir, "templates/")

templates = Jinja2Templates(directory=templates_abs_file_path)

# Simple in‑memory chat history (for demo only)
chat_history = []

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "history": chat_history})

@app.post("/ask")
async def ask(request: Request, task: str = Form(...)):
    """
    Receive a user task, run the runtime, and stream the result back.
    """
    # Run the runtime in a background task so we can stream
    async def run_and_stream():
        # The runtime is synchronous; run it in a thread pool
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            run_task_loop,
            task,
            4  # max_reload_cycles
        )
        # Yield the final result as a single chunk
        yield f"data: {json.dumps({'role':'assistant','content':result or ''})}\n\n"

    # Add user message to history
    chat_history.append({"role":"user","content":task})
    return StreamingResponse(run_and_stream(), media_type="text/event-stream")
# --- NEW: GET /ask ---------------------------------------------------------
@app.get("/ask")
async def ask_get(task: str = Query(None)):
    """
    Optional GET endpoint.
    * If a `task` query parameter is supplied, run the runtime and return the result.
    * If no `task` is supplied, just return a simple health‑check message.
    """
    if task is None:
        # Health‑check / no‑task scenario
        return JSONResponse({"role": "assistant", "content": "Hello, world!"})

    # Record the user message
    chat_history.append({"role": "user", "content": task})

    # Run the runtime in a thread pool
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(
        None,
        run_task_loop,
        task,
        4
    )
    return JSONResponse({"role": "assistant", "content": result or ""})
# ---------------------------------------------------------------------------

