# agent_runtime_new.py
import os
import yaml
import time
import argparse
from typing import List, Dict, Any, Optional
from langgraph.prebuilt import create_react_agent
from langgraph_supervisor import create_supervisor
from langchain.chat_models import init_chat_model
from tools import TOOLS, AGENTS_FOLDER, DB_PATH
from tools import create_task as create_task_tool, list_phases as list_phases_tool, save_artifact as save_artifact_tool, assemble_artifact_html
import sqlite3
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import threading

MODEL_DEFAULT = "ollama:gpt-oss:20b"

# Initialize FastAPI app for message routing
app = FastAPI()
messages = []

@app.get("/messages")
def get_messages():
    """Endpoint to fetch all messages."""
    return JSONResponse(content={"messages": messages})

# ---------- message history helpers ----------
def append_history(history: List[Dict[str, Any]], role: str, content: str, name: Optional[str] = None):
    entry = {"role": role, "content": content}
    if name:
        entry["name"] = name
    history.append(entry)

def history_to_messages_input(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    msgs = []
    for h in history:
        msg = {"role": h["role"], "content": h["content"]}
        if "name" in h:
            msg["name"] = h["name"]
        msgs.append(msg)
    return msgs

# ---------- agent loader ----------
def load_agent_configs(folder_path: str) -> List[Dict[str, Any]]:
    configs = []
    os.makedirs(folder_path, exist_ok=True)
    for fn in sorted(os.listdir(folder_path)):
        if not fn.endswith(".yaml"):
            continue
        path = os.path.join(folder_path, fn)
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
            cfg["_filename"] = fn
            configs.append(cfg)
    return configs

def make_agent_from_config(cfg: Dict[str, Any]):
    name = cfg.get("name") or cfg.get("id") or cfg.get("_filename").replace(".yaml", "")
    model_id = cfg.get("model", MODEL_DEFAULT)
    temp = cfg.get("temperature", 0.2)
    prompt = cfg.get("prompt", "")
    tool_names = cfg.get("tools", []) or []

    selected_tools = []
    for t in tool_names:
        if t in TOOLS:
            selected_tools.append(TOOLS[t])
        else:
            print(f"[loader] WARNING: unknown tool '{t}' referenced in {name}; skipping.")

    llm = init_chat_model(model_id, temperature=temp)

    agent = create_react_agent(
        model=llm,
        tools=selected_tools,
        prompt=prompt,
        name=name
    )
    return agent

def build_agents_from_folder(folder_path: str):
    configs = load_agent_configs(folder_path)
    agents = []
    for cfg in configs:
        try:
            agent = make_agent_from_config(cfg)
            agents.append(agent)
            print(f"[loader] Loaded agent: {cfg.get('name')} from {cfg.get('_filename')}")
        except Exception as e:
            print(f"[loader] Failed to create agent from {cfg.get('_filename')}: {e}")
    return agents

# ---------- DB helper (small) ----------
def _conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def get_task_row(task_id: int):
    conn = _conn()
    cur = conn.cursor()
    cur.execute("SELECT id, external_id, description, status FROM tasks WHERE id=?", (task_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return None
    return {"id": r[0], "external_id": r[1], "description": r[2], "status": r[3]}

# ---------- runtime: run a user task end-to-end ----------
def run_task_loop(user_task: str, external_id: str = None, max_reload_cycles: int = 6):
    """
    Entry:
      - user_task: goal string
    Behavior:
      - ensure agent_manager exists (pre-seed in agents/)
      - start supervisor with agents present
      - send initial user message (task)
      - supervisor will call agent_manager to create agents as needed (AGENT_MGR: marker detection)
      - on AGENT_MGR detection: hot-reload agents, replay history, continue
      - additionally, coordinate high-level phase transitions using DB and review gating
    """
    reload_count = 0
    last_agent_mgr_marker = None
    message_history: List[Dict[str, Any]] = []
    append_history(message_history, "user", user_task)

    # create DB task entry via tool (agent_manager can also do this)
    ext = external_id or f"task_{int(time.time())}"
    task_row = create_task_tool(ext, user_task)
    task_id = task_row["task_id"]
    print(f"[runtime] Created task id={task_id}")

    while True:
        if reload_count > max_reload_cycles:
            print("[runtime] Reached max reload cycles. Exiting.")
            return None

        print(f"\n[runtime] --- Build cycle #{reload_count + 1} ---")
        agents = build_agents_from_folder(AGENTS_FOLDER)
        if not agents:
            print("[runtime] No agents found. Please place agent_manager.yaml into agents/ and retry.")
            return None

        # Supervisor LLM (can be same)
        llm_supervisor = init_chat_model(MODEL_DEFAULT, temperature=0.2)

        supervisor_prompt = """
You are a supervisor that routes work to the appropriate agent(s).
You are not to do the actual work yourself; instead orchestrate agents.
If an agent output contains 'AGENT_MGR:', it signals that the agent manager created/updated YAMLs.
"""

        supervisor = create_supervisor(
            model=llm_supervisor,
            agents=agents,
            prompt=supervisor_prompt,
            add_handoff_back_messages=True,
            output_mode="full_history",
        ).compile()

        print("[runtime] Supervisor compiled with agents:", [a.name for a in agents])

        # Replay canonical history into supervisor
        input_messages = history_to_messages_input(message_history)
        print(f"[runtime] Replaying {len(input_messages)} historical messages into supervisor.")
        stream = supervisor.stream({"messages": input_messages})

        reload_triggered = False
        for chunk in stream:
            node_name, node_update = list(chunk.items())[0]
            msgs = node_update.get("messages", [])
            if not msgs:
                continue
            last = msgs[-1]
            content = getattr(last, "content", str(last))
            who = getattr(last, "name", node_name)

            # Log and send messages to the frontend
            log_message = f"[{node_name}] {who}: {content}"
            print(log_message)
            send_to_frontend(log_message)

            # Detect dynamic tool calls
            if isinstance(content, str) and content.startswith("TOOL_CALL:"):
                tool_name = content.split(":", 1)[1].strip()
                try:
                    result = resolve_and_invoke_tool(tool_name)
                    response_message = f"Tool '{tool_name}' executed successfully: {result}"
                    append_history(message_history, "assistant", response_message, name=who)
                    send_to_frontend(response_message)
                except Exception as e:
                    error_message = f"Error executing tool '{tool_name}': {e}"
                    append_history(message_history, "assistant", error_message, name=who)
                    send_to_frontend(error_message)
                continue

            # Avoid duplicating identical assistant tail message
            is_duplicate = False
            if message_history and message_history[-1].get("content") == content and message_history[-1].get("role") == "assistant":
                is_duplicate = True

            if not is_duplicate:
                append_history(message_history, "assistant", content, name=who)
                send_to_frontend(content)

            # detect agent manager signals
            if isinstance(content, str) and "AGENT_MGR:" in content:
                if content.strip() != last_agent_mgr_marker:
                    last_agent_mgr_marker = content.strip()
                    print("[runtime] Detected AGENT_MGR signal:", content)
                    reload_triggered = True
                    break
                else:
                    print("[runtime] Ignoring identical AGENT_MGR marker.")
                    continue

        if reload_triggered:
            reload_count += 1
            time.sleep(0.25)
            print(f"[runtime] Reload cycle triggered (count={reload_count}). Rebuilding agents and replaying history...")
            continue

        # Normal finish: supervisor produced final content; now run phase progression checks
        # For simplicity, we'll check DB phases and ask the supervisor to run next-phase tasks in the loop externally.
        # This stage returns last assistant output
        final_output = None
        for h in reversed(message_history):
            if h.get("role") == "assistant":
                final_output = h.get("content")
                break
        print("\n[runtime] Supervisor run finished. Last assistant output:\n", final_output)
        return {"task_id": task_id, "final": final_output, "history": message_history}

# Add a dynamic tool/agent resolution function
def resolve_and_invoke_tool(tool_name: str, *args, **kwargs):
    """
    Dynamically resolve and invoke a tool by its name.
    """
    if tool_name not in TOOLS:
        raise ValueError(f"Tool '{tool_name}' not found in the registry.")
    return TOOLS[tool_name](*args, **kwargs)

# Start the FastAPI app in a separate thread
def start_fastapi():
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

threading.Thread(target=start_fastapi, daemon=True).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-agent runtime (stateful hot-reload).")
    parser.add_argument("--task", "-t", type=str, help="Task description (quoted)", required=True)
    parser.add_argument("--external-id", "-e", type=str, help="Optional external task id")
    args = parser.parse_args()
    result = run_task_loop(args.task, external_id=args.external_id, max_reload_cycles=8)
    print("[main] result:", result)
