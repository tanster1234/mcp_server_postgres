# server_test.py
import asyncio
import httpx
import json
import sys
from mcp import ClientSession
from mcp.client.sse import sse_client

async def run(connection_string):
    # Assuming your server is running on localhost:8000
    server_url = "http://localhost:8000/sse"  
    
    try:
        print(f"Connecting to MCP server at {server_url}...")
        if connection_string:
            print(f"Using database connection: {connection_string[:10]}...")
        
        # Create the SSE client context manager
        async with sse_client(url=server_url) as streams:
            print("SSE streams established, creating session...")
            
            # Create and initialize the MCP ClientSession
            async with ClientSession(*streams) as session:
                print("Session created, initializing...")
                
                # Initialize the connection
                await session.initialize()
                print("Connection initialized!")
                
                # List available prompts
                prompts_response = await session.list_prompts()
                print(f"Available prompts: {prompts_response}")
                
                # List available tools
                tools_response = await session.list_tools()
                tools = tools_response.tools
                print(f"Available tools: {[tool.name for tool in tools]}")
                                
                # List available resources
                resources_response = await session.list_resources()
                print(f"Available resources: {resources_response}")

                # List available resource templates
                templates_response = await session.list_resource_templates()
                print(f"Available resource templates: {templates_response}")

                # Try a database connection test if pg_query tool is available
                if any(tool.name == 'pg_query' for tool in tools):
                    try:
                        print("\nTesting database connection with pg_query tool...")
                        result = await session.call_tool(
                            "pg_query", 
                            {
                                "query": "SELECT version() AS version",
                                "connection_string": connection_string
                            }
                        )
                        
                        # Extract version from TextContent
                        if hasattr(result, 'content') and result.content:
                            content = result.content[0]
                            if hasattr(content, 'text'):
                                version_data = json.loads(content.text)
                                print(f"Database connection successful: {version_data.get('version', 'Unknown')}")
                            else:
                                print(f"Database connection successful, but version info not available")
                        else:
                            print("Database query executed but returned no content")
                            
                    except Exception as e:
                        print(f"Database connection test failed: {e}")


    except httpx.HTTPStatusError as e:
        print(f"HTTP Error: {e}")
        print(f"Status code: {e.response.status_code}")
        print(f"Response body: {e.response.text}")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")

if __name__ == "__main__":
    # Get database connection string from command line argument
    connection_string = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run(connection_string))