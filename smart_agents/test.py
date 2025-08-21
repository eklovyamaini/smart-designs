import langchain_core
print(langchain_core.__file__)

from langchain_ollama import ChatOllama
from langchain_core.messages import AIMessage, HumanMessage

# Initialize the Ollama model
model = ChatOllama(model="gpt-oss:20b")  # make sure this matches your local model name

# Minimal prompt with expected structure
# $prop.Type and $prop.capabilities must exist if your template uses them
agent_test_input = {
    "Type": ["test_agent"],
    "capabilities": ["answer basic questions"],
    "name": "minimal_agent",
    "description": "A test agent to verify Ollama call"
}

# Create a human message for the prompt
messages = [
    HumanMessage(content=f"Hello, agent info: {agent_test_input}")
]

try:
    # Call the model
    response = model.invoke(messages)
    print("Ollama response:", response.content)
except Exception as e:
    print("Ollama error:", e)
