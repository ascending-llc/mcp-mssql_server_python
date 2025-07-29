from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
import asyncio


async def my_progress_handler(
        progress: float,
        total: float | None,
        message: str | None
) -> None:
    if total is not None:
        percentage = (progress / total) * 100
        print(f"Progress: {percentage:.1f}% - {message or ''}")
    else:
        print(f"Progress: {progress} - {message or ''}")


async def main():
    try:
        transport = StreamableHttpTransport(url="http://127.0.0.1:8002/mcp")
        client = Client(transport,
                        progress_handler=my_progress_handler,
                        timeout=24 * 60 * 1000)
        async with client:
            result = await client.call_tool(
                "execute_sql",
                {"query": "SELECT Top 100  * from core.v_Reviews"},  # Example query
                # {"query": "select * from SchemaA.View_Student_Classes;"},
            )
            print("Tool result:", result)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    asyncio.run(main())
