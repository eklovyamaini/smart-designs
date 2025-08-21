# tools.py
import os
import yaml
import sqlite3
from difflib import SequenceMatcher
from typing import List, Dict, Any, Optional
from langchain_core.tools import tool
from datetime import datetime
import math

AGENTS_FOLDER = "agents"
DB_PATH = "orchestration.db"
ARTIFACT_CHUNK_SIZE = 2000  # chars per stored artifact part

# --------------- DB helpers ----------------
def _conn():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_id TEXT,
        description TEXT,
        status TEXT,
        created_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        name TEXT,
        status TEXT,
        sequence INTEGER,
        created_at TEXT,
        FOREIGN KEY(task_id) REFERENCES tasks(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER,
        phase_id INTEGER,
        name TEXT,
        part_index INTEGER,
        content TEXT,
        created_at TEXT,
        FOREIGN KEY(task_id) REFERENCES tasks(id),
        FOREIGN KEY(phase_id) REFERENCES phases(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artifact_id INTEGER,
        reviewer TEXT,
        score REAL,
        comments TEXT,
        created_at TEXT,
        FOREIGN KEY(artifact_id) REFERENCES artifacts(id)
    )""")
    conn.commit()
    conn.close()

# Initialize DB at import
init_db()



REQUIRED_FIELDS = {
    "Type": ["unknown"],        # must be a non-empty list
    "capabilities": ["none"],   # default if missing
}

def validate_agent_yaml(yaml_folder: str) -> None:
    """
    Validate all YAML agent definitions in the folder.
    Add default values for missing required fields to prevent Ollama template errors.
    """
    for filename in os.listdir(yaml_folder):
        if not filename.endswith(".yaml") and not filename.endswith(".yml"):
            continue

        path = os.path.join(yaml_folder, filename)
        with open(path, "r") as f:
            agent_data = yaml.safe_load(f) or {}

        modified = False
        for field, default_value in REQUIRED_FIELDS.items():
            if field not in agent_data or not agent_data[field]:
                agent_data[field] = default_value
                modified = True

        if modified:
            print(f"[VALIDATOR] Updated missing fields in {filename}")
            with open(path, "w") as f:
                yaml.safe_dump(agent_data, f)

def validate_runtime_agents(agent_states: list) -> None:
    """
    Validate runtime agent states before sending to Ollama.
    Handles both dicts and CompiledStateGraph objects.
    """
    for agent in agent_states:
        # extract state dict if agent is a CompiledStateGraph
        state = getattr(agent, "state", None) if not isinstance(agent, dict) else agent
        if state is None:
            continue  # skip if no state to validate

        for field, default_value in REQUIRED_FIELDS.items():
            if field not in state or not state[field]:
                state[field] = default_value

def patch_runtime_agents(agents: list):
    """
    Ensure every runtime agent (CompiledStateGraph) has required fields for Ollama templates.
    This prevents slice/index errors.
    """
    for agent in agents:
        state = getattr(agent, "state", None)
        if state is None:
            continue
        for field, default_value in REQUIRED_FIELDS.items():
            # Fill only if missing or empty
            if not hasattr(state, field) and field not in state:
                state[field] = default_value
            elif field in state and not state[field]:
                state[field] = default_value

# --------------- basic operational tools --------------
@tool
def add(a: float, b: float):
    "Add two numbers."
    return a + b

@tool
def multiply(a: float, b: float):
    "Multiply two numbers."
    return a * b

@tool
def save_note(text: str) -> str:
    "Save a short note to a local file under data/."
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", "note.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return f"saved: {path}"

# --------------- artifact & task tools ----------------
def _now_iso():
    return datetime.utcnow().isoformat() + "Z"

def create_task(external_id: str, description: str) -> Dict[str, Any]:
    """
    Create a new task row and seed default phases (requirements->design->architecture->impl->test->deploy).
    Returns the new task row as dict.
    """
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO tasks (external_id, description, status, created_at) VALUES (?, ?, ?, ?)",
                (external_id, description, "IN_PROGRESS", _now_iso()))
    task_id = cur.lastrowid

    default_phases = [
        ("requirements", 0),
        ("domain_review", 1),
        ("business_design", 2),
        ("enterprise_architecture", 3),
        ("detailed_design", 4),
        ("implementation", 5),
        ("testing", 6),
        ("deployment", 7),
    ]
    for name, seq in default_phases:
        cur.execute("INSERT INTO phases (task_id, name, status, sequence, created_at) VALUES (?, ?, ?, ?, ?)",
                    (task_id, name, "PENDING", seq, _now_iso()))
    conn.commit()
    conn.close()
    return {"task_id": task_id, "external_id": external_id, "description": description}

@tool
def create_task_tool(external_id: str, description: str) -> Dict[str, Any]:
    """
    LangChain tool wrapper for creating a new task and seeding default phases.
    Uses the existing `create_task` DB function.
    """
    return create_task(external_id, description)

@tool
def get_task(task_id: int) -> Dict[str, Any]:
    "gets the task details from persistent storage."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, external_id, description, status, created_at FROM tasks WHERE id=?", (task_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"error": "not_found", "task_id": task_id}
    return {"id": row[0], "external_id": row[1], "description": row[2], "status": row[3], "created_at": row[4]}

@tool
def list_phases(task_id: int) -> List[Dict[str, Any]]:
    "gets the phases for a task from persistent storage."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, status, sequence FROM phases WHERE task_id=? ORDER BY sequence", (task_id,))
    rows = cur.fetchall()
    conn.close()
    return [{"id": r[0], "name": r[1], "status": r[2], "sequence": r[3]} for r in rows]

@tool
def update_phase_status(phase_id: int, status: str) -> str:
    "Updates the status of a phase in persistent storage."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE phases SET status=? WHERE id=?", (status, phase_id))
    conn.commit()
    conn.close()
    return f"phase_{phase_id}_status_set_to_{status}"

@tool
def save_artifact(task_id: int, phase_id: int, name: str, content: str) -> Dict[str, Any]:
    """
    Save a (potentially large) artifact by chunking it into parts.
    Returns artifact meta with part count and id of first part.
    """
    conn = _conn()
    cur = conn.cursor()
    # remove previous artifact parts with same name for this phase if present
    cur.execute("DELETE FROM artifacts WHERE task_id=? AND phase_id=? AND name=?", (task_id, phase_id, name))
    total_len = len(content)
    parts = math.ceil(total_len / ARTIFACT_CHUNK_SIZE) if total_len > 0 else 1
    part_id_first = None
    for i in range(parts):
        start = i * ARTIFACT_CHUNK_SIZE
        chunk = content[start:start+ARTIFACT_CHUNK_SIZE]
        cur.execute("INSERT INTO artifacts (task_id, phase_id, name, part_index, content, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (task_id, phase_id, name, i, chunk, _now_iso()))
        if part_id_first is None:
            part_id_first = cur.lastrowid
    conn.commit()
    conn.close()
    return {"task_id": task_id, "phase_id": phase_id, "name": name, "parts": parts, "first_part_id": part_id_first}

@tool
def get_artifact_parts(task_id: int, phase_id: int, name: str) -> List[str]:
    "gets the artifact parts for a specific task and phase from persistent storage."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT content FROM artifacts WHERE task_id=? AND phase_id=? AND name=? ORDER BY part_index", (task_id, phase_id, name))
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]

@tool
def assemble_artifact_html(task_id: int, phase_id: int, name: str) -> str:
    "assembles the artifact parts into a single HTML document."
    parts = get_artifact_parts(task_id, phase_id, name)
    html = "<!doctype html><html><head><meta charset='utf-8'><title>{}</title></head><body>{}</body></html>".format(name, "".join(parts))
    return html

@tool
def record_review(artifact_id: int, reviewer: str, score: float, comments: str) -> Dict[str, Any]:
    "Records a review for an artifact in persistent storage."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO reviews (artifact_id, reviewer, score, comments, created_at) VALUES (?, ?, ?, ?, ?)",
                (artifact_id, reviewer, score, comments, _now_iso()))
    conn.commit()
    rid = cur.lastrowid
    conn.close()
    return {"review_id": rid, "artifact_id": artifact_id, "reviewer": reviewer, "score": score}

@tool
def aggregate_reviews_for_artifact(artifact_id: int) -> Dict[str, Any]:
    "Gets the average score and count of reviews for a specific artifact."
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT AVG(score), COUNT(*) FROM reviews WHERE artifact_id=?", (artifact_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return {"error": "no_reviews", "artifact_id": artifact_id}
    avg = row[0] or 0.0
    count = row[1] or 0
    return {"artifact_id": artifact_id, "average_score": avg, "count": count}

# --------------- YAML agent management tools --------------
@tool
def list_agents() -> list:
    "List all agent YAML definition files in the agents folder."
    os.makedirs(AGENTS_FOLDER, exist_ok=True)
    return [f for f in os.listdir(AGENTS_FOLDER) if f.endswith(".yaml")]

@tool
def read_agent_yaml(filename: str) -> dict:
    "Read and return an agent definition from YAML."
    path = os.path.join(AGENTS_FOLDER, filename)
    if not os.path.exists(path):
        return {"error": "File not found", "filename": filename}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@tool
def write_agent_yaml(filename: str, definition: dict) -> str:
    """
    Write or update an agent definition YAML.
    If file exists, it will be overwritten.
    """
    os.makedirs(AGENTS_FOLDER, exist_ok=True)
    path = os.path.join(AGENTS_FOLDER, filename)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(definition, f, sort_keys=False)
    # Return a clear machine-detectable string
    return f"AGENT_MGR: CREATED_OR_UPDATED {filename}"

@tool
def ensure_agent_definition(name: str, requirements: str,  threshold: float = 0.7) -> str:
    """
    Check existing agent YAMLs for similarity to requirements (prompt+tools).
    If a match above `threshold` is found, update it and return a marker.
    Otherwise, create a new YAML file and return a marker.
    The return strings all begin with 'AGENT_MGR:' so the runtime can detect them.
    """
    
    print(f"\n\n\n\n\n\n\n[AGENT_MGR] Ensuring agent definition for '{name}' with requirements: {requirements} and tools: {tools}\n\n\n\n\n\n\n")
    os.makedirs(AGENTS_FOLDER, exist_ok=True)
    tools = tools or []
    best_match = None
    best_score = 0.0

    for f in list_agents():
        try:
            with open(os.path.join(AGENTS_FOLDER, f), "r", encoding="utf-8") as fh:
                content = yaml.safe_load(fh) or {}
        except Exception:
            continue
        text_existing = (content.get("prompt", "") or "") + " " + " ".join(content.get("tools", []) or [])
        score = SequenceMatcher(None, text_existing.lower(), requirements.lower()).ratio()
        if score > best_score:
            best_score = score
            best_match = f

    if best_score >= threshold and best_match:
        # Update the best match's prompt (requirements) and tools (merge)
        path = os.path.join(AGENTS_FOLDER, best_match)
        with open(path, "r", encoding="utf-8") as fh:
            content = yaml.safe_load(fh) or {}
        content["prompt"] = requirements
        # Merge unique tools
        existing_tools = content.get("tools", []) or []
        merged_tools = list(dict.fromkeys(existing_tools + tools))
        content["tools"] = merged_tools
        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(content, fh, sort_keys=False)
        return f"AGENT_MGR: UPDATED {best_match} (score={best_score:.2f})"

    # Otherwise create new
    safe_name = name.replace(" ", "_").lower()
    new_file = f"{safe_name}.yaml"
    new_def = {
        "name": name,
        "model": "ollama:gpt-oss:20b",
        "temperature": 0.2,
        "prompt": requirements,
        "tools": tools or []
    }
    with open(os.path.join(AGENTS_FOLDER, new_file), "w", encoding="utf-8") as fh:
        yaml.safe_dump(new_def, fh, sort_keys=False)
    return f"AGENT_MGR: CREATED {new_file} (no match above {threshold})"

# --------------- export mapping ----------------
TOOLS = {
    # operational
    "add": add,
    "multiply": multiply,
    "save_note": save_note,
    # persistence
    "create_task_tool": create_task_tool,
    "get_task": get_task,
    "list_phases": list_phases,
    "update_phase_status": update_phase_status,
    "save_artifact": save_artifact,
    "get_artifact_parts": get_artifact_parts,
    "assemble_artifact_html": assemble_artifact_html,
    "record_review": record_review,
    "aggregate_reviews_for_artifact": aggregate_reviews_for_artifact,
    # agent management
    "list_agents": list_agents,
    "read_agent_yaml": read_agent_yaml,
    "write_agent_yaml": write_agent_yaml,
    "ensure_agent_definition": ensure_agent_definition,
}
