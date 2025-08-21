# multi_agent_runtime.py
import os
import yaml
import time
from typing import List, Dict, Any, Optional

from langgraph.prebuilt import create_react_agent
from langgraph_supervisor import create_supervisor
from langchain.chat_models import init_chat_model
from tools import TOOLS, AGENTS_FOLDER

AGENTS_FOLDER = AGENTS_FOLDER  # from tools.py
MODEL_DEFAULT = "ollama:gpt-oss:20b"

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

# ----------------------------
# Utilities for message history
# ----------------------------
def append_history(history: List[Dict[str, Any]], role: str, content: str, name: Optional[str] = None):
    """
    Append one message entry to the canonical history.
    History entries are dicts with at least 'role' and 'content'. Optionally 'name' (agent name).
    """
    entry = {"role": role, "content": content}
    if name:
        entry["name"] = name
    history.append(entry)

def history_to_messages_input(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Convert canonical history entries into the format expected by supervisor.stream input:
      [{"role":"user"/"assistant"/"system", "content":"..."} ...]
    We keep 'name' in messages as an additional field if present (LangGraph reads it).
    """
    msgs = []
    for h in history:
        msg = {"role": h["role"], "content": h["content"]}
        # include name if present (some runtimes use it)
        if "name" in h:
            msg["name"] = h["name"]
        msgs.append(msg)
    return msgs

# ----------------------------
# Main loop with true hot-reload and stateful continuation
# ----------------------------
def run_with_stateful_hot_reload(user_task: str, max_reload_cycles: int = 3):
    """
    - Builds agents and supervisor.
    - Streams the supervisor while recording canonical message history.
    - If agent_manager writes/updates YAMLs (detected by AGENT_MGR: marker),
      it rebuilds agents and supervisor and then replays the full history into the new supervisor,
      allowing the new supervisor to continue from the same conversation state.
    - Stops after max_reload_cycles reloads.
    """
    reload_count = 0
    last_agent_mgr_marker = None  # to avoid reacting to identical repeated markers
    # canonical message history: start with a system message optionally
    message_history: List[Dict[str, Any]] = []
    # Seed conversation with the initial user message
    append_history(message_history, "user", user_task)

    while True:
        if reload_count > max_reload_cycles:
            print("[runtime] Reached max reload cycles. Exiting.")
            return None

        print(f"\n[runtime] --- Build cycle #{reload_count + 1} ---")
        agents = build_agents_from_folder(AGENTS_FOLDER)
        if not agents:
            print("[runtime] No agents found. Add agent YAMLs to agents/ and retry.")
            return None

        llm_supervisor = init_chat_model(MODEL_DEFAULT, temperature=0.2)

        supervisor_prompt = """
        You are a supervisor that routes work to the appropriate agent(s). Provide detailed instructions to respective agent(s) as needed.
        Do not do the work yourself. If an agent writes a response starting with 'AGENT_MGR:', treat it as a signal that the agent manager created/updated agent YAMLs.
        """

        supervisor = create_supervisor(
            model=llm_supervisor,
            agents=agents,
            prompt=supervisor_prompt,
            add_handoff_back_messages=True,
            output_mode="full_history",
            recursion_limit= "50"
        ).compile()

        print("[runtime] Supervisor compiled with agents:", [a.name for a in agents])

        # Prepare input messages for the supervisor from canonical message_history
        input_messages = history_to_messages_input(message_history)
        print("[runtime] Replaying history into supervisor (messages count = {})".format(len(input_messages)))

        # Stream the supervisor continuing from the same message history
        stream = supervisor.stream({"messages": input_messages},config={"recursion_limit": 50})

        reload_triggered = False
        # We'll collect any new assistant messages and append them to history as they come
        for chunk in stream:
            # chunk is a dict like { "node_identifier": { "messages": [...] } }
            node_name, node_update = list(chunk.items())[0]
            msgs = node_update.get("messages", [])
            if not msgs:
                continue
            last = msgs[-1]
            content = getattr(last, "content", str(last))
            who = getattr(last, "name", node_name)  # agent name if present

            # The stream may give us repeated past messages; only append NEW content not already in history tail.
            # We'll consider a simple heuristic: if the last history entry exactly matches (role=user/assistant + content),
            # skip appending duplicate. Otherwise append as an assistant message.
            is_duplicate = False
            if message_history:
                last_hist = message_history[-1]
                # If the content and role match, it's likely a duplicate echo from replay; skip.
                if last_hist.get("content") == content and last_hist.get("role") == "assistant":
                    is_duplicate = True

            if not is_duplicate:
                append_history(message_history, "assistant", content, name=who)

            # Detect agent manager signals
            if isinstance(content, str) and "AGENT_MGR:" in content:
                # guard against reacting to the exact same marker repeatedly
                if content.strip() != last_agent_mgr_marker:
                    print("[runtime] Detected AGENT_MGR signal:", content)
                    last_agent_mgr_marker = content.strip()
                    reload_triggered = True
                    # Important: keep the agent manager's output in history (it was appended above)
                    # Break the stream so we can reload agents & supervisor
                    break
                else:
                    print("[runtime] Ignoring repeated identical AGENT_MGR marker.")
                    # continue streaming (do not trigger reload)
                    continue

        # If reload_triggered, we will rebuild agents and replay the full message_history
        if reload_triggered:
            reload_count += 1
            # small delay to ensure filesystem flush
            time.sleep(0.25)
            print(f"[runtime] Reloading agents (reload_count={reload_count}) and continuing from preserved history...")
            # loop continues which will rebuild and replay using the updated message_history
            continue

        # If we exhausted stream without encountering AGENT_MGR, the supervisor finished normally.
        # Return the last assistant message content as final output.
        final_output = None
        # Find last assistant message in message_history
        for h in reversed(message_history):
            if h.get("role") == "assistant":
                final_output = h.get("content")
                break

        print("\n[runtime] Supervisor finished normally. Final output:\n", final_output)
        return final_output


if __name__ == "__main__":
    # Example task: ask agent_manager to create a new 'qa_agent'
    user_task = (
        "Please create or update an agent named 'qa_agent' whose job is to answer short factual questions. "
        "Requirements: keep answers under 30 words, provide a source when asked, and have the tools 'list_agents' and 'read_agent_yaml' available. "
        "If you create/update a YAML, return the tool response starting with 'AGENT_MGR:'"
    )

    result = run_with_stateful_hot_reload(user_task, max_reload_cycles=4)
    print("[main] result:", result)
