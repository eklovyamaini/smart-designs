import litellm
import asyncio

async def main():
    response = await litellm.acompletion(
        model="ollama/gpt-oss:20b",
        messages=[{"role":"user","content":"Hello"}],
    )
    print(response)

if __name__ == "__main__":
    asyncio.run(main())