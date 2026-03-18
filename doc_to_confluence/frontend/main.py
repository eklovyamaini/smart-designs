"""
FastAPI web frontend for the Word-to-Confluence migration tool.
Port: 8001

Run with:
    uvicorn doc_to_confluence.frontend.main:app --port 8001 --reload

Endpoints:
    GET  /              → 3-step wizard HTML
    POST /parse         → Upload .docx, returns flat section list
    POST /build-config  → Validate config dict, return YAML download
    POST /migrate       → SSE stream: run migration with live progress
"""
import asyncio
import json
import os
import queue
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from fastapi import FastAPI, File, Form, Query, Request, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

# ─── Defaults file paths ─────────────────────────────────────────────────────
_DEFAULTS_PATH          = Path(__file__).parent / "defaults.yaml"
_SECTION_DEFAULTS_PATH  = Path(__file__).parent / "section_defaults.yaml"

# ─── sys.path: ensure repo root is importable ────────────────────────────────
REPO_ROOT = str(Path(__file__).parent.parent.parent.resolve())
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from doc_to_confluence.config import MigrationConfigModel
from doc_to_confluence.confluence_client import ConfluenceClient
from doc_to_confluence.metadata_manager import MetadataManager
from doc_to_confluence.models import MigrationReport, ParsedSection, SectionResult
from doc_to_confluence.orchestrator import (
    MigrationOrchestrator,
    _flatten_sections,
    _now_iso,
)
from doc_to_confluence.parser import parse_docx

# ─── App Setup ────────────────────────────────────────────────────────────────

FRONTEND_DIR = Path(__file__).parent
app = FastAPI(title="Doc to Confluence Migration", docs_url=None, redoc_url=None)

app.mount(
    "/static",
    StaticFiles(directory=str(FRONTEND_DIR / "static")),
    name="static",
)
templates = Jinja2Templates(directory=str(FRONTEND_DIR / "templates"))

# Module-level upload store: file_id -> absolute path on disk
_upload_store: Dict[str, str] = {}


# ─── Request Models ───────────────────────────────────────────────────────────

class BuildConfigRequest(BaseModel):
    config: dict


class MigrateRequest(BaseModel):
    file_id: str
    dry_run: bool = True
    overwrite: bool = False
    pre_delete: bool = False  # wipe module folder hierarchy before migration
    config: dict


class SaveDefaultsRequest(BaseModel):
    confluence_base_url: str = ""
    confluence_user: str = ""
    confluence_api_token: str = ""
    default_space_key: str = ""
    llm_model: str = "gpt-oss:20b"
    llm_temperature: float = 0.1
    max_llm_workers: int = 4
    plantuml_theme: str = "cerulean"
    metadata_default_approvers: str = ""


class SaveSectionDefaultsRequest(BaseModel):
    raw_yaml: str


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/parse")
async def parse_endpoint(file: UploadFile = File(...)) -> JSONResponse:
    """
    Upload a .docx file, save it to a temp directory, and parse it.
    Returns the flat section list and a file_id for subsequent calls.
    """
    filename = file.filename or "document.docx"
    if not filename.lower().endswith(".docx"):
        return JSONResponse(
            status_code=400,
            content={"error": "Only .docx files are supported."},
        )

    # Save to a temp directory
    tmp_dir = tempfile.mkdtemp(prefix="doc2confluence_")
    safe_name = Path(filename).name  # prevent path traversal
    tmp_path = os.path.join(tmp_dir, safe_name)

    content = await file.read()
    with open(tmp_path, "wb") as f:
        f.write(content)

    # Register with a UUID
    file_id = uuid.uuid4().hex
    _upload_store[file_id] = tmp_path

    # Parse document
    try:
        top_level = parse_docx(tmp_path)
        flat_sections = _flatten_sections(top_level)
    except Exception as exc:
        return JSONResponse(
            status_code=422,
            content={"error": f"Failed to parse document: {exc}"},
        )

    # Serialize (TypedDicts are plain dicts - already JSON-serialisable).
    # Strip children from the flat view to avoid redundancy; all descendants
    # already appear as siblings.
    def _strip_children(s: ParsedSection) -> dict:
        d = dict(s)
        d["children"] = []
        # Convert nested TableData to plain dicts (they already are)
        return d

    sections_json = [_strip_children(s) for s in flat_sections]

    return JSONResponse({
        "file_id": file_id,
        "filename": safe_name,
        "sections": sections_json,
    })


@app.post("/build-config")
async def build_config(body: BuildConfigRequest) -> Response:
    """
    Validate the config dict via Pydantic and return a downloadable YAML file.
    """
    try:
        validated = MigrationConfigModel(**body.config)
    except Exception as exc:
        return JSONResponse(
            status_code=422,
            content={"error": str(exc)},
        )

    config_dict = validated.model_dump()
    yaml_str = yaml.dump(
        config_dict,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )

    return Response(
        content=yaml_str,
        media_type="application/x-yaml",
        headers={"Content-Disposition": "attachment; filename=migration_config.yaml"},
    )


@app.get("/defaults")
async def get_defaults() -> JSONResponse:
    """
    Return the current defaults from defaults.yaml.
    Supports ${ENV_VAR_NAME} substitution so credentials can live in env vars.
    """
    try:
        raw = _load_defaults_raw()
        confluence = raw.get("confluence", {})
        llm = raw.get("llm", {})

        from doc_to_confluence.config import _substitute_env_vars
        confluence = _substitute_env_vars(confluence)
        llm        = _substitute_env_vars(llm)

        plantuml  = _substitute_env_vars(raw.get("plantuml", {}))
        metadata  = _substitute_env_vars(raw.get("metadata", {}))
        return JSONResponse({
            "confluence_base_url":        confluence.get("base_url", ""),
            "confluence_user":            confluence.get("user", ""),
            "confluence_api_token":       confluence.get("api_token", ""),
            "default_space_key":          confluence.get("default_space_key", ""),
            "llm_model":                  llm.get("model", "gpt-oss:20b"),
            "llm_temperature":            llm.get("temperature", 0.1),
            "max_llm_workers":            llm.get("max_workers", 4),
            "plantuml_theme":             plantuml.get("theme", "cerulean"),
            "metadata_default_approvers": metadata.get("default_approvers", ""),
        })
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to load defaults: {exc}"},
        )


@app.post("/defaults")
async def save_defaults(body: SaveDefaultsRequest) -> JSONResponse:
    """
    Persist updated defaults back to defaults.yaml.
    Blank api_token values are NOT overwritten — preserves ${ENV_VAR} references.
    """
    try:
        # Read the existing file to preserve comments and env-var placeholders
        raw = _load_defaults_raw()

        # Only overwrite api_token if the caller actually sent a non-blank value
        # that isn't an env-var placeholder. This means clearing the token in the
        # UI (submitting empty string) keeps the existing file value intact.
        existing_token = raw.get("confluence", {}).get("api_token", "")
        new_token = body.confluence_api_token.strip()
        # If the UI sent empty, keep whatever was there (env-var ref or blank)
        final_token = new_token if new_token else existing_token

        raw["confluence"] = {
            "base_url":          body.confluence_base_url.strip(),
            "user":              body.confluence_user.strip(),
            "api_token":         final_token,
            "default_space_key": body.default_space_key.strip(),
        }
        raw["llm"] = {
            "model":       body.llm_model.strip() or "gpt-oss:20b",
            "temperature": body.llm_temperature,
            "max_workers": max(1, body.max_llm_workers),
        }
        raw["plantuml"] = {
            "theme": body.plantuml_theme.strip() or "cerulean",
        }
        raw["metadata"] = {
            "default_approvers": body.metadata_default_approvers.strip(),
        }

        with open(_DEFAULTS_PATH, "w", encoding="utf-8") as f:
            f.write(_DEFAULTS_HEADER)
            yaml.dump(raw, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

        return JSONResponse({"ok": True})
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to save defaults: {exc}"},
        )


def _load_defaults_raw() -> dict:
    """Read defaults.yaml and return the raw (un-substituted) dict."""
    if not _DEFAULTS_PATH.exists():
        return {}
    with open(_DEFAULTS_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_DEFAULTS_HEADER = """\
# ─────────────────────────────────────────────────────────────────
#  doc_to_confluence — Frontend Defaults
#
#  These values pre-fill the Global Settings panel in the browser.
#  The user can still override any value live in the UI without
#  saving; clicking "Save Defaults" writes changes back here.
#
#  Supports ${ENV_VAR_NAME} substitution — e.g. use
#    api_token: ${CONFLUENCE_API_TOKEN}
#  to avoid storing credentials in this file.
# ─────────────────────────────────────────────────────────────────

"""


@app.get("/section-defaults")
async def get_section_defaults() -> JSONResponse:
    """Return the raw YAML text and parsed rules list from section_defaults.yaml."""
    try:
        if _SECTION_DEFAULTS_PATH.exists():
            with open(_SECTION_DEFAULTS_PATH, "r", encoding="utf-8") as f:
                raw = f.read()
        else:
            raw = ""
        parsed = yaml.safe_load(raw) or {}
        sd = parsed.get("section_defaults", {})
        # Support both old list format and new dict format {context_vars, rules}
        if isinstance(sd, list):
            config = {"context_vars": {}, "rules": sd}
        else:
            config = {
                "context_vars": sd.get("context_vars", {}),
                "rules": sd.get("rules", []),
            }
        return JSONResponse({"raw_yaml": raw, "config": config, "rules": config["rules"]})
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to load section defaults: {exc}"},
        )


@app.post("/section-defaults")
async def save_section_defaults(body: SaveSectionDefaultsRequest) -> JSONResponse:
    """Validate and persist section_defaults.yaml."""
    try:
        parsed = yaml.safe_load(body.raw_yaml) or {}
    except Exception as exc:
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid YAML: {exc}"},
        )
    try:
        with open(_SECTION_DEFAULTS_PATH, "w", encoding="utf-8") as f:
            f.write(body.raw_yaml)
        sd = parsed.get("section_defaults", {})
        if isinstance(sd, list):
            config = {"context_vars": {}, "rules": sd}
        else:
            config = {
                "context_vars": sd.get("context_vars", {}),
                "rules": sd.get("rules", []),
            }
        return JSONResponse({"ok": True, "config": config, "rules": config["rules"]})
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to save section defaults: {exc}"},
        )


@app.post("/migrate")
async def migrate_endpoint(body: MigrateRequest) -> StreamingResponse:
    """
    SSE streaming endpoint. Runs the migration orchestrator in a thread pool
    and emits section progress events in real-time.
    """
    # Validate file_id
    file_path = _upload_store.get(body.file_id)
    if not file_path or not os.path.exists(file_path):
        return JSONResponse(  # type: ignore[return-value]
            status_code=404,
            content={"error": "File not found. Please re-upload the document."},
        )

    # Validate config
    try:
        config = MigrationConfigModel(**body.config)
    except Exception as exc:
        return JSONResponse(  # type: ignore[return-value]
            status_code=422,
            content={"error": str(exc)},
        )

    dry_run    = body.dry_run
    overwrite  = body.overwrite
    pre_delete = body.pre_delete
    result_queue: queue.Queue = queue.Queue()

    # Build streaming orchestrator
    orchestrator = _build_streaming_orchestrator(result_queue, config, dry_run, overwrite, pre_delete)

    async def event_stream():
        loop = asyncio.get_running_loop()

        # Run the orchestrator in a thread pool executor
        future = loop.run_in_executor(None, orchestrator.run, file_path, "")

        # Drain queue while orchestrator is running
        while not future.done():
            try:
                event = await loop.run_in_executor(
                    None, lambda: result_queue.get(timeout=0.3)
                )
                if event is None:
                    break  # sentinel: orchestrator finished emitting
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                continue

        # Drain any remaining items
        while not result_queue.empty():
            try:
                event = result_queue.get_nowait()
                if event is None:
                    continue
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                break

        # Check for exceptions from the orchestrator thread
        exc = future.exception()
        if exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
            return

        # Emit the final complete event
        try:
            report = future.result()
            yield f"data: {json.dumps({'type': 'complete', 'report': _serialize_report(report)}, default=str)}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ─── Metadata Manager Endpoints ──────────────────────────────────────────────

@app.get("/metadata", response_class=HTMLResponse)
async def metadata_page(request: Request) -> HTMLResponse:
    """Render the Metadata Manager UI."""
    return templates.TemplateResponse("metadata.html", {"request": request})


@app.post("/metadata/preview")
async def metadata_preview(request: Request) -> JSONResponse:
    """
    Return all Confluence pages in scope (descendants of each parent URL).

    Expects JSON body:
        {
          "parent_urls": ["https://org.atlassian.net/wiki/spaces/DS/pages/123/..."],
          "confluence_base_url": "https://org.atlassian.net",
          "confluence_user": "user@example.com",
          "confluence_api_token": "ATATT3x..."
        }

    Returns:
        { "pages": [{page_id, title, url, has_blocks, error}, ...] }
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    base_url         = body.get("confluence_base_url", "").strip()
    user             = body.get("confluence_user", "").strip()
    api_token        = body.get("confluence_api_token", "").strip()
    parent_urls: List[str] = body.get("parent_urls", [])

    if not base_url or not user or not api_token:
        return JSONResponse(
            status_code=422,
            content={"error": "confluence_base_url, confluence_user, and confluence_api_token are required"},
        )
    if not parent_urls:
        return JSONResponse(status_code=422, content={"error": "parent_urls must not be empty"})

    try:
        client = ConfluenceClient(base_url=base_url, user=user, api_token=api_token)
        mgr    = MetadataManager(client=client, base_url=base_url)
        pages  = mgr.preview_scope(parent_urls)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    return JSONResponse({"pages": pages})


@app.get("/metadata/find-module-pages")
async def metadata_find_module_pages(
    space_key:              str = Query(...),
    confluence_base_url:    str = Query(...),
    confluence_user:        str = Query(...),
    confluence_api_token:   str = Query(...),
) -> JSONResponse:
    """
    Return all Module pages in a Confluence space (pages whose title ends
    with "- Module" or "– Module"), for use in auto-populating the parent
    URL list.

    Query parameters:
        space_key, confluence_base_url, confluence_user, confluence_api_token

    Returns:
        { "pages": [{id, title, url}, ...] }
    """
    if not space_key:
        return JSONResponse(status_code=422, content={"error": "space_key is required"})
    if not confluence_base_url or not confluence_user or not confluence_api_token:
        return JSONResponse(
            status_code=422,
            content={"error": "confluence_base_url, confluence_user, and confluence_api_token are required"},
        )
    try:
        client = ConfluenceClient(
            base_url=confluence_base_url,
            user=confluence_user,
            api_token=confluence_api_token,
        )
        mgr   = MetadataManager(client=client, base_url=confluence_base_url)
        pages = mgr.find_module_pages(space_key)
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    return JSONResponse({"pages": pages})


@app.post("/metadata/create-tracker")
async def metadata_create_tracker(request: Request) -> JSONResponse:
    """
    Create or update a Confluence tracking page containing the Page Properties
    Report macro that aggregates review metadata across all pages in the space.

    Expects JSON body:
        {
          "space_key": "DS",
          "tracker_title": "DS Review Tracking Dashboard",
          "parent_page_id": null,
          "confluence_base_url": "https://org.atlassian.net",
          "confluence_user": "user@example.com",
          "confluence_api_token": "ATATT3x..."
        }

    Returns:
        { "ok": true, "page_id": "...", "title": "...", "url": "..." }
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    base_url       = body.get("confluence_base_url", "").strip()
    user           = body.get("confluence_user", "").strip()
    api_token      = body.get("confluence_api_token", "").strip()
    space_key      = body.get("space_key", "").strip()
    tracker_title  = body.get("tracker_title", "DS Review Tracking Dashboard").strip()
    parent_page_id = body.get("parent_page_id") or None
    label          = body.get("label", "ds-tracked").strip() or "ds-tracked"

    if not base_url or not user or not api_token:
        return JSONResponse(
            status_code=422,
            content={"error": "confluence_base_url, confluence_user, and confluence_api_token are required"},
        )
    if not space_key:
        return JSONResponse(status_code=422, content={"error": "space_key is required"})

    try:
        client = ConfluenceClient(base_url=base_url, user=user, api_token=api_token)
        mgr    = MetadataManager(client=client, base_url=base_url)
        result = mgr.create_or_update_tracker_page(
            space_key=space_key,
            tracker_title=tracker_title,
            parent_page_id=parent_page_id,
            label=label,
        )
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})

    return JSONResponse({"ok": True, **result})


@app.post("/metadata/apply")
async def metadata_apply(
    parent_urls: List[str] = Form(...),
    force: str = Form("false"),
    confluence_base_url: str = Form(...),
    confluence_user: str = Form(...),
    confluence_api_token: str = Form(...),
    default_approvers: str = Form(""),
    label: str = Form("ds-tracked"),
    include_properties: str = Form("true"),
    include_change_history: str = Form("true"),
    include_labels: str = Form("true"),
) -> StreamingResponse:
    """
    SSE streaming endpoint. Applies metadata blocks to all pages in scope.

    Emits JSON-encoded SSE events:
        data: {"type": "start",    "total": N}
        data: {"type": "progress", "page_id", "title", "status", "current", "total"}
        data: {"type": "complete", "applied", "skipped", "errors", "total"}
        data: {"type": "error",    "message": "..."}
    """
    force_bool               = force.lower()               in ("true", "1", "yes")
    include_properties_bool  = include_properties.lower()  in ("true", "1", "yes")
    include_change_history_bool = include_change_history.lower() in ("true", "1", "yes")
    include_labels_bool      = include_labels.lower()      in ("true", "1", "yes")

    try:
        client = ConfluenceClient(
            base_url=confluence_base_url,
            user=confluence_user,
            api_token=confluence_api_token,
        )
        mgr = MetadataManager(client=client, base_url=confluence_base_url)
    except Exception as exc:
        async def _err():
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
        return StreamingResponse(_err(), media_type="text/event-stream")

    result_queue: queue.Queue = queue.Queue()

    def _run_in_thread():
        try:
            for event in mgr.apply_to_scope(
                parent_urls,
                force=force_bool,
                default_approvers=default_approvers,
                label=label,
                include_properties=include_properties_bool,
                include_change_history=include_change_history_bool,
                include_labels=include_labels_bool,
            ):
                result_queue.put(event)
        except Exception as exc:
            result_queue.put({"type": "error", "message": str(exc)})
        finally:
            result_queue.put(None)  # sentinel

    async def event_stream():
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(None, _run_in_thread)

        while not future.done():
            try:
                event = await loop.run_in_executor(
                    None, lambda: result_queue.get(timeout=0.3)
                )
                if event is None:
                    break
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                continue

        # Drain any remaining events
        while not result_queue.empty():
            try:
                event = result_queue.get_nowait()
                if event is None:
                    continue
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                break

        if future.exception():
            yield f"data: {json.dumps({'type': 'error', 'message': str(future.exception())})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/metadata/auto-label")
async def metadata_auto_label(request: Request) -> StreamingResponse:
    """
    SSE streaming endpoint. Adds smart module + page-type labels to all
    descendant pages under the supplied parent URLs.

    Accepts a JSON body::

        {
          "confluence_base_url":  "https://org.atlassian.net",
          "confluence_user":      "user@example.com",
          "confluence_api_token": "...",
          "parent_urls":          ["https://org.atlassian.net/wiki/..."]
        }

    Emits JSON-encoded SSE events:
        data: {"type": "start",    "total": N}
        data: {"type": "progress", "page_id", "title", "new_labels",
                                   "already_had", "status", "current", "total"}
        data: {"type": "complete", "labeled", "unchanged", "errors", "total"}
        data: {"type": "error",    "message": "..."}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})

    base_url  = body.get("confluence_base_url", "").strip()
    user      = body.get("confluence_user", "").strip()
    api_token = body.get("confluence_api_token", "").strip()
    parent_urls: List[str] = body.get("parent_urls", [])

    if not base_url or not user or not api_token:
        return JSONResponse(
            status_code=422,
            content={"error": "confluence_base_url, confluence_user, and confluence_api_token are required"},
        )
    if not parent_urls:
        return JSONResponse(
            status_code=422,
            content={"error": "parent_urls must not be empty"},
        )

    try:
        client = ConfluenceClient(base_url=base_url, user=user, api_token=api_token)
        mgr    = MetadataManager(client=client, base_url=base_url)
    except Exception as exc:
        async def _err():
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
        return StreamingResponse(_err(), media_type="text/event-stream")

    result_queue: queue.Queue = queue.Queue()

    def _run_in_thread():
        try:
            for event in mgr.auto_label_scope(parent_urls):
                result_queue.put(event)
        except Exception as exc:
            result_queue.put({"type": "error", "message": str(exc)})
        finally:
            result_queue.put(None)  # sentinel

    async def event_stream():
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(None, _run_in_thread)

        while not future.done():
            try:
                event = await loop.run_in_executor(
                    None, lambda: result_queue.get(timeout=0.3)
                )
                if event is None:
                    break
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                continue

        # Drain any remaining events
        while not result_queue.empty():
            try:
                event = result_queue.get_nowait()
                if event is None:
                    continue
                yield f"data: {json.dumps(event, default=str)}\n\n"
            except queue.Empty:
                break

        if future.exception():
            yield f"data: {json.dumps({'type': 'error', 'message': str(future.exception())})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


# ─── StreamingOrchestrator ────────────────────────────────────────────────────

def _build_streaming_orchestrator(
    result_queue: queue.Queue,
    config: MigrationConfigModel,
    dry_run: bool,
    overwrite: bool = False,
    pre_delete: bool = True,
) -> "StreamingOrchestrator":
    """Factory to build a StreamingOrchestrator with injected queue."""

    class StreamingOrchestrator(MigrationOrchestrator):
        """
        Subclass of MigrationOrchestrator that emits SSE events into a queue
        as each section mapping is processed.

        Overrides run() to replicate the mapping loop with queue emissions.
        orchestrator.py is NOT modified.
        """

        def __init__(self, rq: queue.Queue, **kwargs):
            self._rq = rq
            super().__init__(**kwargs)

        def _pre_delete_module_pages(self) -> None:
            """Override to stream deletion progress via the SSE queue."""
            client = self._get_confluence()

            # Collect unique (space_key, root_folder_title) pairs
            roots: set = set()
            for mapping in self._config.sections:
                space_key   = mapping.confluence.space_key
                folder_path = mapping.confluence.folder_path
                if not space_key or not folder_path:
                    continue
                first_segment = folder_path.split("/")[0].strip()
                if first_segment:
                    roots.add((space_key, first_segment))

            if not roots:
                self._rq.put({"type": "status", "message": "Pre-delete: no folders configured, skipping."})
                return

            for space_key, root_title in sorted(roots):
                self._rq.put({"type": "status", "message": f"Pre-delete: looking for '{root_title}' in space '{space_key}'…"})
                try:
                    root_page = client.get_page_by_title(space_key, root_title)
                except Exception as exc:
                    print(f"[orchestrator] pre_delete: WARN: lookup failed for '{root_title}': {exc}")
                    continue

                if root_page is None:
                    self._rq.put({"type": "status", "message": f"Pre-delete: '{root_title}' not found — nothing to delete."})
                    continue

                root_id = root_page["id"]
                try:
                    descendants = client.get_all_descendants(root_id)
                except Exception as exc:
                    print(f"[orchestrator] pre_delete: WARN: could not fetch descendants of '{root_title}': {exc}")
                    continue

                total_to_delete = len(descendants) + 1
                self._rq.put({"type": "status", "message": f"Pre-delete: deleting {total_to_delete} page(s) under '{root_title}'…"})

                deleted = 0
                for page in reversed(descendants):
                    try:
                        client.delete_page(page["id"])
                        deleted += 1
                        if deleted % 10 == 0:
                            self._rq.put({"type": "status", "message": f"Pre-delete: deleted {deleted}/{total_to_delete} pages under '{root_title}'…"})
                    except Exception as exc:
                        print(f"[orchestrator] pre_delete: WARN: could not delete '{page['title']}': {exc}")

                try:
                    client.delete_page(root_id)
                    deleted += 1
                except Exception as exc:
                    print(f"[orchestrator] pre_delete: WARN: could not delete root '{root_title}': {exc}")

                self._rq.put({"type": "status", "message": f"Pre-delete: done — {deleted} page(s) removed under '{root_title}'."})

        def run(self, doc_path: str, config_path: str = "") -> MigrationReport:  # type: ignore[override]
            started_at = _now_iso()
            print(f"[streaming_orchestrator] Starting: {doc_path}")
            if self._dry_run:
                print("[streaming_orchestrator] DRY RUN")

            # Emit immediately so the UI exits "Connecting…" state
            self._rq.put({"type": "status", "message": "Connected — preparing migration…"})

            # Step 0: Pre-delete module folder hierarchy (if enabled and not dry-run)
            if self._pre_delete and not self._dry_run:
                print("[streaming_orchestrator] pre_delete=True: wiping existing module pages...")
                self._pre_delete_module_pages()

            # Parse document
            top_level = parse_docx(doc_path)
            all_sections = _flatten_sections(top_level)
            print(f"[streaming_orchestrator] Parsed {len(all_sections)} sections")

            # ── Document section+table audit ──────────────────────────────────
            print("[audit] Document structure:")
            for s in all_sections:
                indent = "  " * max(0, s["level"] - 1)
                print(f"  [doc] {indent}[{s['id']}] H{s['level']} '{s['title']}' — {len(s.get('tables', []))} table(s), {len(s.get('children', []))} child(ren)")
                for t_idx, tbl in enumerate(s.get("tables", [])):
                    rows = tbl.get("rows", [])
                    hdr = rows[0] if rows else []
                    print(f"  [doc] {indent}  table[{t_idx}]: {len(rows)} rows | headers: {hdr}")
            # ── end document audit ─────────────────────────────────────────────

            results: List[SectionResult] = []
            matched_section_ids: set = set()
            total = len(self._config.sections)

            # ── Pre-compute inherited_folder for each mapping (serial pass) ──────
            # This mirrors the folder_stack logic in orchestrator.run() so that
            # section mappings processed in parallel still receive the correct
            # inherited folder context.
            _pre_folder_stack: list = []   # [(level, name), …]
            enriched: list = []            # (i, mapping, inherited_folder)
            for i, mapping in enumerate(self._config.sections):
                inherited_folder = "/".join(n for _, n in _pre_folder_stack) or None
                enriched.append((i, mapping, inherited_folder))
                if mapping.confluence.folder_only:
                    folder_name = mapping.confluence.folder_path or ""
                    if folder_name:
                        lvl = mapping.level or 1
                        while _pre_folder_stack and _pre_folder_stack[-1][0] > lvl:
                            _pre_folder_stack.pop()
                        _pre_folder_stack.append((lvl, folder_name))

            # ── Partition mappings into three execution groups ────────────────────
            # folder_only  → serial first  (creates the Confluence folder hierarchy)
            # create/update → parallel      (independent pages; the expensive phase)
            # append        → serial last   (must append to already-created pages)
            folder_items = [(i, m, f) for i, m, f in enriched if m.confluence.folder_only]
            create_items = [(i, m, f) for i, m, f in enriched
                            if not m.confluence.folder_only and m.confluence.action != "append"]
            append_items = [(i, m, f) for i, m, f in enriched
                            if not m.confluence.folder_only and m.confluence.action == "append"]

            def _run_one(args):
                i, mapping, inherited_folder = args
                self._rq.put({
                    "type": "section_start",
                    "mapping_match": mapping.match,
                    "index": i,
                    "total": total,
                })
                result = self._process_mapping(
                    mapping, all_sections, inherited_folder=inherited_folder
                )
                self._rq.put({
                    "type": "section_result",
                    "result": dict(result),
                })
                return result

            # ── Phase 1: folder-only sections (serial) ───────────────────────────
            for args in folder_items:
                result = _run_one(args)
                if result.get("section_id"):
                    matched_section_ids.add(result["section_id"])
                results.append(result)

            # ── Phase 2: independent create/update sections (parallel) ────────────
            with ThreadPoolExecutor(max_workers=self._config.max_llm_workers) as pool:
                for result in pool.map(_run_one, create_items):
                    if result.get("section_id"):
                        matched_section_ids.add(result["section_id"])
                    results.append(result)

            # ── Phase 3: append-action sections (serial) ─────────────────────────
            for args in append_items:
                result = _run_one(args)
                if result.get("section_id"):
                    matched_section_ids.add(result["section_id"])
                results.append(result)

            # ── Unmatched-section audit ────────────────────────────────────────
            unmatched = [s for s in all_sections if s["id"] not in matched_section_ids]
            if unmatched:
                print(f"\n[audit] {len(unmatched)} section(s) not matched by any mapping:")
                for s in unmatched:
                    indent = "  " * max(0, s["level"] - 1)
                    tbl_info = f"{len(s['tables'])} table(s)" if s.get("tables") else "no tables"
                    print(f"  [audit] {indent}UNMATCHED [{s['id']}] H{s['level']} '{s['title']}' — {tbl_info}")
                    for t_idx, tbl in enumerate(s.get("tables", [])):
                        rows = tbl.get("rows", [])
                        hdr = rows[0] if rows else []
                        print(f"  [audit] {indent}  table[{t_idx}]: {len(rows)} rows | headers: {hdr}")
            else:
                print("\n[audit] All document sections were matched by a mapping.")
            # ── end unmatched audit ────────────────────────────────────────────

            finished_at = _now_iso()
            report = MigrationReport(
                doc_path=os.path.abspath(doc_path),
                config_path=config_path,
                total_sections_in_doc=len(all_sections),
                total_mappings=total,
                results=results,
                dry_run=self._dry_run,
                started_at=started_at,
                finished_at=finished_at,
            )

            if self._config.db_logging:
                self._log_report_to_db(report)

            # Sentinel signals the async generator that we're done emitting
            self._rq.put(None)
            return report

    return StreamingOrchestrator(rq=result_queue, config=config, dry_run=dry_run, verbose=False, overwrite=overwrite, pre_delete=pre_delete)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _serialize_report(report: MigrationReport) -> dict:
    """Convert MigrationReport (TypedDict with nested TypedDicts) to a plain dict."""
    d = dict(report)
    d["results"] = [dict(r) for r in d.get("results", [])]
    return d


# ─── Dev Entry Point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("doc_to_confluence.frontend.main:app", host="127.0.0.1", port=8001, reload=True)
