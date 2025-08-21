import datetime
import logging
from zoneinfo import ZoneInfo
from google.adk.agents import Agent
from google.adk.tools import AgentTool
from google.adk.models.lite_llm import LiteLlm

import os
import yaml

# --- Logging Setup ---
log_file_path = os.path.join(os.path.dirname(__file__), 'agent_debug.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file_path,
    filemode='w'  # Overwrite the log file on each run
)
logging.info("Logger initialized.")
# --- End Logging Setup ---

from .tools import  FileSystemTools

def load_agents(agents_folder: str) -> dict:
    """Loads all agent definitions from YAML files in the specified folder."""
    agents = {}
    agent_data = {}

    # Pass 1: Read all yaml files into a dictionary, searching recursively
    for root, _, files in os.walk(agents_folder):
        for file_name in files:
            if file_name.endswith('.yaml'):
                agent_name = os.path.splitext(file_name)[0]
                file_path = os.path.join(root, file_name)
                with open(file_path, 'r') as file:
                    data = yaml.safe_load(file)
                if data:
                    agent_data[agent_name] = data

    # Define the root directory for file operations.
    file_op_root = os.path.dirname(os.path.abspath(agents_folder))
    
    # Instantiate our new, simple file system tools.
    file_system_tools = FileSystemTools(root_dir=file_op_root)

    # Pass 2: Create all agent objects using the data from the YAML files
    for agent_name, data in agent_data.items():
        py_tools = [globals()[tool_name] for tool_name in (data.get('tools') or []) if tool_name in globals()]
        
        # Only the scrum_master gets the file system tools.
        if agent_name == 'scrum_master':
            py_tools.extend([
                file_system_tools.write_file,
                file_system_tools.read_file,
                file_system_tools.list_directory
            ])

        agents[agent_name] = Agent(
            name=data['name'],
            model=LiteLlm(
                model="ollama/gemma3:27b",
                api_base="http://localhost:11434",
                force_prompt_template=True,
                json_mode=False  # Explicitly disable JSON mode
            ),
            description=data['description'],
            instruction=data['instruction'],
            tools=py_tools,
        )
        logging.info(f"--- Instruction for {agent_name} ---")
        logging.info(data['instruction'])
        logging.info("--------------------")
        logging.info(f"Agent '{agent_name}' created with tools: {[tool.__name__ for tool in py_tools]}")

    # Pass 3: Wire up agent-as-tool dependencies
    for agent_name, data in agent_data.items():
        agent_to_update = agents.get(agent_name)
        if not agent_to_update:
            continue
        for tool_name in (data.get('tools') or []):
            if tool_name in agents: # If the tool is another agent
                tool_agent = agents[tool_name]
                agent_to_update.tools.append(AgentTool(agent=tool_agent))
                logging.info(f"Added Agent '{tool_name}' as a tool to Agent '{agent_name}'.")

    # Pass 4: Dynamically provide all other agents as tools to the scrum_master
    #scrum_master = agents.get('scrum_master')
    #if scrum_master:
    #    for agent_name, agent in agents.items():
    #        if agent_name != 'scrum_master':
    #            scrum_master.tools.append(AgentTool(agent=agent))
    #            logging.info(f"Added Agent '{agent_name}' as a tool to Agent 'scrum_master'.")

    return agents

# Load all agents from the YAML files
agents_folder = os.path.join(os.path.dirname(__file__), 'agents')
agents = load_agents(agents_folder)
root_agent = agents.get('scrum_master')
logging.info(f"Root agent set to: '{root_agent.name if root_agent else 'None'}'")