import asyncio
from a2a.multi_tool_agent.agent import root_agent

async def main():
    """Runs the root agent to generate the project artifacts."""
    if not root_agent:
        print("Error: root_agent not found.")
        return

    while True:
        prompt = input("Enter your command for the root_agent: ")
        if prompt.lower() in ["exit", "quit"]:
            break

        # We need to create a context for the agent to run in.
        # Since I cannot import InvocationContext, I will create a simple dictionary
        # to pass the prompt to the agent.
        context = {"prompt": prompt}
        
        async for event in root_agent.run_async(context):
            if event.get('type') == 'agent_thought':
                print(f"\n{'='*20}\n{event['agent_name']} is thinking...\n{'='*20}\n")
            elif event.get('type') == 'tool_code':
                print(f"\n{'='*20}\n{event['agent_name']} is using tool: {event['tool_name']}\nInput:\n{event['input']}\n{'='*20}\n")
            elif event.get('type') == 'tool_output':
                print(f"\n{'='*20}\nTool {event['tool_name']} output:\n{event['output']}\n{'='*20}\n")
            elif event.get('type') == 'agent_output':
                print(f"\n{'='*20}\n{event['agent_name']} says:\n{event['output']}\n{'='*20}\n")

if __name__ == '__main__':
    asyncio.run(main())
