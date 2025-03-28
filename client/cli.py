# client/cli
import asyncio
import dotenv
import os
import sys
import json
import uuid
import urllib.parse
from pydantic_ai import Agent
from mcp import ClientSession
from mcp.client.sse import sse_client

def postgres_connection_to_uuid(connection_string, namespace=uuid.NAMESPACE_URL):
    """
    Convert a PostgreSQL connection string into a deterministic Version 5 UUID.
    Includes both connection credentials (netloc) and database name (path).
    
    Args:
        connection_string: Full PostgreSQL connection string
        namespace: UUID namespace (default is URL namespace)
        
    Returns:
        str: UUID representing the connection
    """
    # Make sure connection_string has proper protocol prefix
    if not connection_string.startswith("postgresql://"):
        connection_string = f"postgresql://{connection_string}"
        
    # Parse the connection string
    parsed = urllib.parse.urlparse(connection_string)
    
    # Extract the netloc (user:password@host:port) and path (database name)
    connection_id_string = parsed.netloc + parsed.path
    
    # Create a Version 5 UUID (SHA-1 based)
    result_uuid = uuid.uuid5(namespace, connection_id_string)
    
    return str(result_uuid)

async def fetch_schema_info(session, db_url):
    """Fetch database schema information from the MCP server."""
    schema_info = []
    
    # Generate connection ID from the connection string
    conn_id = postgres_connection_to_uuid(db_url)
    
    # List all tables using pgmcp protocol
    try:
        table_resource = f"pgmcp://{conn_id}/tables"
        tables_response = await session.read_resource(table_resource)
        
        if hasattr(tables_response, 'content') and tables_response.content:
            content = tables_response.content[0]
            if hasattr(content, 'text'):
                tables = json.loads(content.text)
                
                # For each table, get its columns
                for table in tables:
                    schema_name = table.get('table_schema')
                    table_name = table.get('table_name')
                    table_description = table.get('description', '')
                    
                    # Fetch columns for this table
                    columns_resource = f"pgmcp://{conn_id}/tables/{schema_name}/{table_name}/columns"
                    columns_response = await session.read_resource(columns_resource)
                    
                    columns = []
                    if hasattr(columns_response, 'content') and columns_response.content:
                        content = columns_response.content[0]
                        if hasattr(content, 'text'):
                            columns = json.loads(content.text)
                    
                    # Add table with its columns to schema info
                    schema_info.append({
                        'schema': schema_name,
                        'table': table_name,
                        'description': table_description,
                        'columns': columns
                    })
        
        return schema_info
    except Exception as e:
        print(f"Error fetching schema information: {e}")
        return []

def format_schema_for_prompt(schema_info):
    """Format schema information as a string for the prompt."""
    if not schema_info:
        return "No schema information available."
    
    schema_text = "DATABASE SCHEMA:\n\n"
    
    for table_info in schema_info:
        schema_name = table_info.get('schema')
        table_name = table_info.get('table')
        description = table_info.get('description', '')
        
        schema_text += f"Table: {schema_name}.{table_name}"
        if description:
            schema_text += f" - {description}"
        schema_text += "\n"
        
        columns = table_info.get('columns', [])
        if columns:
            schema_text += "Columns:\n"
            for col in columns:
                col_name = col.get('column_name', '')
                data_type = col.get('data_type', '')
                is_nullable = col.get('is_nullable', '')
                description = col.get('description', '')
                
                schema_text += f"  - {col_name} ({data_type}, nullable: {is_nullable})"
                if description:
                    schema_text += f" - {description}"
                schema_text += "\n"
        
        schema_text += "\n"
    
    return schema_text

async def main():
    # Load environment variables
    dotenv.load_dotenv()
    anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
    db_url = os.getenv('DATABASE_URL')
    pg_mcp_url = "http://localhost:8000/sse"
    
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    
    if not anthropic_api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable is not set.")
        sys.exit(1)
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python cli-client.py 'your natural language query'")
        sys.exit(1)
    
    user_query = sys.argv[1]
    print(f"Processing query: {user_query}")
    
    # First, connect to MCP server to get schema information
    try:
        print(f"Connecting to MCP server at {pg_mcp_url} to fetch schema information...")
        
        # Create the SSE client context manager
        async with sse_client(url=pg_mcp_url) as streams:
            print("SSE streams established, creating session...")
            
            # Create and initialize the MCP ClientSession
            async with ClientSession(*streams) as session:
                print("Session created, initializing...")
                
                # Initialize the connection
                await session.initialize()
                print("Connection initialized!")
                
                # Fetch schema information
                print("Fetching database schema information...")
                schema_info = await fetch_schema_info(session, db_url)
                schema_text = format_schema_for_prompt(schema_info)
                
                print(f"Retrieved information for {len(schema_info)} tables.")
                
                # Generate SQL using Claude with schema context
                system_prompt = """You are an expert at converting natural language to SQL. 
Given the database schema below, generate a PostgreSQL SQL query to answer the user's question.
Output ONLY the SQL query with no explanations or markdown formatting.

{schema}
"""
                formatted_system_prompt = system_prompt.format(schema=schema_text)
                
                print("Generating SQL query with Claude...")
                agent = Agent(  
                    "anthropic:claude-3-5-haiku-latest",
                    system_prompt=formatted_system_prompt,  
                )
                
                result = agent.run_sync(user_query)
                sql_query = result.data
                print(f"\nGenerated SQL query:")
                print(f"------------------")
                print(sql_query)
                print(f"------------------\n")
                
                # Execute the generated SQL query
                print("Executing SQL query...")
                try:
                    # Get connection ID for the connection string
                    conn_id = postgres_connection_to_uuid(db_url)
                    
                    result = await session.call_tool(
                        "pg_query", 
                        {
                            "query": sql_query,
                            "connection_string": db_url  # Still use connection string for tool
                        }
                    )
                    
                    # Extract and format results
                    if hasattr(result, 'content') and result.content:
                        content = result.content[0]
                        if hasattr(content, 'text'):
                            query_results = json.loads(content.text)
                            print("\nQuery Results:")
                            print("==============")
                            if query_results:
                                # Pretty print the results
                                if isinstance(query_results, list) and len(query_results) > 0:
                                    # Print column headers
                                    headers = query_results[0].keys()
                                    header_row = ' | '.join(str(h) for h in headers)
                                    separator = '-' * len(header_row)
                                    print(header_row)
                                    print(separator)
                                    
                                    # Print data rows
                                    for row in query_results:
                                        print(' | '.join(str(row.get(h, '')) for h in headers))
                                    
                                    print(f"\nTotal rows: {len(query_results)}")
                                else:
                                    print(json.dumps(query_results, indent=2))
                            else:
                                print("Query executed successfully but returned no results.")
                        else:
                            print("Query executed but returned an unexpected format.")
                    else:
                        print("Query executed but returned no content.")
                except Exception as e:
                    print(f"Error executing SQL query: {type(e).__name__}: {e}")
                
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())