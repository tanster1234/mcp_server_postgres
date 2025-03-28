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
                        
                        # Test the schemas resource
                        print("\nTesting schemas resource...")
                        resource_path = f"pgmcp://{conn_id}/schemas"
                        print(f"Resource path: {resource_path}")
                        
                        # Read the resource - handle both 'content' and 'contents' attributes
                        response = await session.read_resource(resource_path)
                        
                        # Get content regardless of attribute name
                        # the MCP Python SDK responses with a contents attribute for resource endpoints
                        # and a content attribute for tool endpoints
                        response_content = None
                        if hasattr(response, 'content') and response.content:
                            response_content = response.content
                        elif hasattr(response, 'contents') and response.contents:
                            response_content = response.contents
                        
                        # Process the response
                        if response_content:
                            content_item = response_content[0]
                            if hasattr(content_item, 'text'):
                                schemas_data = json.loads(content_item.text)
                                print(f"Successfully retrieved schemas - found {len(schemas_data)} schemas")
                                # Print first few schemas as example
                                for i, schema in enumerate(schemas_data[:3]):
                                    print(f"  - {schema.get('schema_name')}")
                                    if i >= 2 and len(schemas_data) > 3:
                                        print(f"  ... and {len(schemas_data) - 3} more")
                                        break
                                
                                # If we have schemas, test a schema with tables
                                if schemas_data:
                                    # Try schemas until we find one with tables
                                    for schema_idx, schema in enumerate(schemas_data[:5]):  # Try up to 5 schemas
                                        schema_name = schema.get('schema_name')
                                        
                                        print(f"\nTesting tables access for schema {schema_name}...")
                                        tables_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables"
                                        tables_response = await session.read_resource(tables_resource)
                                        
                                        # Get content regardless of attribute name
                                        tables_content = None
                                        if hasattr(tables_response, 'content') and tables_response.content:
                                            tables_content = tables_response.content
                                        elif hasattr(tables_response, 'contents') and tables_response.contents:
                                            tables_content = tables_response.contents
                                        
                                        # Process the tables response
                                        if tables_content:
                                            content_item = tables_content[0]
                                            if hasattr(content_item, 'text'):
                                                tables_data = json.loads(content_item.text)
                                                print(f"Successfully retrieved tables - found {len(tables_data)} tables")
                                                
                                                # Break out of the loop if we found tables
                                                if tables_data:
                                                    # Print first few tables as example
                                                    for i, table in enumerate(tables_data[:3]):
                                                        print(f"  - {table.get('table_name')}")
                                                        if i >= 2 and len(tables_data) > 3:
                                                            print(f"  ... and {len(tables_data) - 3} more")
                                                            break
                                                    
                                                    # Take the first table as a sample
                                                    first_table = tables_data[0]
                                                    table_name = first_table.get('table_name')
                                                    
                                                    # Test columns for this table
                                                    print(f"\nTesting column access for {schema_name}.{table_name}...")
                                                    columns_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables/{table_name}/columns"
                                                    columns_response = await session.read_resource(columns_resource)
                                                    
                                                    # Get content regardless of attribute name
                                                    columns_content = None
                                                    if hasattr(columns_response, 'content') and columns_response.content:
                                                        columns_content = columns_response.content
                                                    elif hasattr(columns_response, 'contents') and columns_response.contents:
                                                        columns_content = columns_response.contents
                                                    
                                                    # Process the columns response
                                                    if columns_content:
                                                        content_item = columns_content[0]
                                                        if hasattr(content_item, 'text'):
                                                            columns_data = json.loads(content_item.text)
                                                            print(f"Successfully retrieved columns - found {len(columns_data)} columns")
                                                            # Print first few columns as example
                                                            for i, col in enumerate(columns_data[:3]):
                                                                print(f"  - {col.get('column_name')} ({col.get('data_type')})")
                                                                if i >= 2 and len(columns_data) > 3:
                                                                    print(f"  ... and {len(columns_data) - 3} more")
                                                                    break
                                                    
                                                    # Test sample data for this table
                                                    print(f"\nTesting sample data access for {schema_name}.{table_name}...")
                                                    sample_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables/{table_name}/sample"
                                                    sample_response = await session.read_resource(sample_resource)
                                                    
                                                    # Get content regardless of attribute name
                                                    sample_content = None
                                                    if hasattr(sample_response, 'content') and sample_response.content:
                                                        sample_content = sample_response.content
                                                    elif hasattr(sample_response, 'contents') and sample_response.contents:
                                                        sample_content = sample_response.contents
                                                    
                                                    # Process the sample response
                                                    if sample_content:
                                                        content_item = sample_content[0]
                                                        if hasattr(content_item, 'text'):
                                                            sample_data = json.loads(content_item.text)
                                                            print(f"Successfully retrieved sample data - found {len(sample_data)} rows")
                                                            # Don't print the actual rows to avoid sensitive data
                                                            print("  (Sample data retrieved successfully)")
                                                    
                                                    # Test rowcount for this table
                                                    print(f"\nTesting rowcount access for {schema_name}.{table_name}...")
                                                    rowcount_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables/{table_name}/rowcount"
                                                    rowcount_response = await session.read_resource(rowcount_resource)
                                                    
                                                    # Get content regardless of attribute name
                                                    rowcount_content = None
                                                    if hasattr(rowcount_response, 'content') and rowcount_response.content:
                                                        rowcount_content = rowcount_response.content
                                                    elif hasattr(rowcount_response, 'contents') and rowcount_response.contents:
                                                        rowcount_content = rowcount_response.contents
                                                    
                                                    # Process the rowcount response
                                                    if rowcount_content:
                                                        content_item = rowcount_content[0]
                                                        if hasattr(content_item, 'text'):
                                                            rowcount_data = json.loads(content_item.text)
                                                            if rowcount_data and len(rowcount_data) > 0:
                                                                print(f"Successfully retrieved row count: {rowcount_data[0].get('approximate_row_count', 'Unknown')}")
                                                    
                                                    # We found a table and tested it, so break out of the schema loop
                                                    break
                                                else:
                                                    print(f"Schema {schema_name} has no tables. Trying another schema if available.")
                                            else:
                                                print(f"No text content in tables response for schema {schema_name}")
                                        else:
                                            print(f"No tables data found for schema {schema_name}")
                                        
                                        # If we've tried all schemas and none had tables, report it
                                        if schema_idx == min(4, len(schemas_data) - 1) and not tables_data:
                                            print("No tables found in any of the tested schemas")
                            else:
                                print("Content doesn't have 'text' attribute")
                        else:
                            print("No content found in the response")
                            
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