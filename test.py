# test.py
import asyncio
import httpx
import json
import sys
import urllib.parse
import uuid
from mcp import ClientSession
from mcp.client.sse import sse_client

async def run(connection_string: str | None):
    """Test the MCP server with an optional database connection string."""
    # Assuming your server is running on localhost:8000
    server_url = "http://localhost:8000/sse"  
    
    try:
        print(f"Connecting to MCP server at {server_url}...")
        if connection_string:
            # Clean and sanitize the connection string
            clean_connection = connection_string.strip()
            # Only show a small part of the connection string for security
            masked_conn_string = clean_connection[:10] + "..." if len(clean_connection) > 10 else clean_connection
            print(f"Using database connection: {masked_conn_string}")
        
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

                # Try a database connection test if pg_query tool is available and connection_string provided
                if connection_string and any(tool.name == 'pg_query' for tool in tools):
                    try:
                        # Use the cleaned connection string
                        clean_connection = connection_string.strip()
                        
                        print("\nTesting database connection with pg_query tool...")
                        result = await session.call_tool(
                            "pg_query", 
                            {
                                "query": "SELECT version() AS version",
                                "connection_string": clean_connection
                            }
                        )
                        
                        # Extract version from TextContent
                        if hasattr(result, 'content') and result.content:
                            content = result.content[0]
                            if hasattr(content, 'text'):
                                # Safely parse JSON
                                try:
                                    version_data = json.loads(content.text)
                                    if isinstance(version_data, list) and len(version_data) > 0:
                                        print(f"Database connection successful: {version_data[0].get('version', 'Unknown')}")
                                    else:
                                        print(f"Database connection successful: {version_data.get('version', 'Unknown')}")
                                except json.JSONDecodeError:
                                    print(f"Database query succeeded but returned non-JSON response: {content.text[:100]}")
                            else:
                                print(f"Database connection successful, but version info not available")
                        else:
                            print("Database query executed but returned no content")
                    except Exception as e:
                        print(f"Database connection test failed: {e}")
                elif connection_string:
                    print("\nThe pg_query tool is not available on the server")
                else:
                    print("\nNo connection string provided, skipping database connection test")

                # Also test a resource if connection_string is provided
                if connection_string:
                    try:
                        # First, we need to register the connection to get a conn_id
                        print("\nRegistering connection string to get conn_id...")
                        result = await session.call_tool(
                            "pg_query", 
                            {
                                "query": "SELECT 1",  # Simple query just to register the connection
                                "connection_string": clean_connection
                            }
                        )
                        
                        # At this point, the connection has been registered in the server
                        # We need to determine the conn_id, which is a deterministic UUID based on the connection
                        # Parse the connection string to extract netloc and path
                        parsed = urllib.parse.urlparse(clean_connection)
                        # The path typically starts with a slash
                        connection_id_string = parsed.netloc + parsed.path
                        # Create a Version 5 UUID (same algorithm as in your Database class)
                        conn_id = str(uuid.uuid5(uuid.NAMESPACE_URL, connection_id_string))
                        
                        print(f"Calculated connection ID: {conn_id}")
                        
                        print("\nTesting resource access for tables...")
                        resource_path = f"pgmcp://{conn_id}/tables"
                        print(f"Resource path: {resource_path}")
                        
                        # Read the resource
                        response = await session.read_resource(resource_path)
                        
                        # Process the response based on the actual structure
                        if hasattr(response, 'contents') and response.contents:
                            content = response.contents[0]
                            if hasattr(content, 'text'):
                                tables_data = json.loads(content.text)
                                print(f"Successfully retrieved tables - found {len(tables_data)} tables")
                                # Print first few tables as example
                                for i, table in enumerate(tables_data[:3]):
                                    print(f"  - {table.get('table_schema')}.{table.get('table_name')}")
                                    if i >= 2 and len(tables_data) > 3:
                                        print(f"  ... and {len(tables_data) - 3} more")
                                        break
                            else:
                                print("Content doesn't have 'text' attribute")
                        else:
                            print("No contents found in the response")
                            
                        # Test getting columns for a specific table if tables were obtained
                        if hasattr(response, 'contents') and response.contents:
                            content = response.contents[0]
                            if hasattr(content, 'text'):
                                tables_data = json.loads(content.text)
                                if tables_data:
                                    # Take the first table as a sample
                                    sample_table = tables_data[0]
                                    schema_name = sample_table.get('table_schema')
                                    table_name = sample_table.get('table_name')
                                    
                                    print(f"\nTesting column access for {schema_name}.{table_name}...")
                                    columns_resource = f"pgmcp://{conn_id}/tables/{schema_name}/{table_name}/columns"
                                    columns_response = await session.read_resource(columns_resource)
                                    
                                    if hasattr(columns_response, 'contents') and columns_response.contents:
                                        content = columns_response.contents[0]
                                        if hasattr(content, 'text'):
                                            columns_data = json.loads(content.text)
                                            print(f"Successfully retrieved columns - found {len(columns_data)} columns")
                                            # Print first few columns as example
                                            for i, col in enumerate(columns_data[:3]):
                                                print(f"  - {col.get('column_name')} ({col.get('data_type')})")
                                                if i >= 2 and len(columns_data) > 3:
                                                    print(f"  ... and {len(columns_data) - 3} more")
                                                    break
                                        else:
                                            print("No text content in columns response")
                                    else:
                                        print("No columns found or resource returned empty result")
                                        
                                    # Test sample data for the same table
                                    print(f"\nTesting sample data access for {schema_name}.{table_name}...")
                                    sample_resource = f"pgmcp://{conn_id}/tables/{schema_name}/{table_name}/sample"
                                    sample_response = await session.read_resource(sample_resource)
                                    
                                    if hasattr(sample_response, 'contents') and sample_response.contents:
                                        content = sample_response.contents[0]
                                        if hasattr(content, 'text'):
                                            sample_data = json.loads(content.text)
                                            print(f"Successfully retrieved sample data - found {len(sample_data)} rows")
                                            # Don't print the actual rows to avoid sensitive data
                                            print("  (Sample data retrieved successfully)")
                                        else:
                                            print("No text content in sample data response")
                                    else:
                                        print("No sample data found or resource returned empty result")
                    except Exception as e:
                        print(f"Error accessing resources: {e}")

    except httpx.HTTPStatusError as e:
        print(f"HTTP Error: {e}")
        print(f"Status code: {e.response.status_code}")
        print(f"Response body: {e.response.text}")
    except httpx.ConnectError:
        print(f"Connection Error: Could not connect to server at {server_url}")
        print("Make sure the server is running and the URL is correct")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")

if __name__ == "__main__":
    # Get database connection string from command line argument
    connection_string = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(run(connection_string))