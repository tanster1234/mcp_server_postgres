# client/claude_cli.py
import asyncio
import dotenv
import os
import sys
import codecs
import json
import uuid
import urllib.parse
import anthropic
from mcp import ClientSession
from mcp.client.sse import sse_client

# This function is no longer needed as we're using the connect tool
# Kept for reference but not used
def postgres_connection_to_uuid(connection_string, namespace=uuid.NAMESPACE_URL):
    """
    Convert a PostgreSQL connection string into a deterministic Version 5 UUID.
    Includes both connection credentials (netloc) and database name (path).
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

async def fetch_schema_info(session, conn_id):
    """Fetch database schema information from the MCP server."""
    schema_info = []
    
    # First get all schemas
    try:
        schemas_resource = f"pgmcp://{conn_id}/schemas"
        schemas_response = await session.read_resource(schemas_resource)
        
        schemas_content = None
        if hasattr(schemas_response, 'content') and schemas_response.content:
            schemas_content = schemas_response.content
        elif hasattr(schemas_response, 'contents') and schemas_response.contents:
            schemas_content = schemas_response.contents
            
        if schemas_content:
            content = schemas_content[0]
            if hasattr(content, 'text'):
                schemas = json.loads(content.text)
                
                # For each schema, get its tables
                for schema in schemas:
                    schema_name = schema.get('schema_name')
                    schema_description = schema.get('description', '')
                    
                    # Fetch tables for this schema
                    tables_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables"
                    tables_response = await session.read_resource(tables_resource)
                    
                    tables_content = None
                    if hasattr(tables_response, 'content') and tables_response.content:
                        tables_content = tables_response.content
                    elif hasattr(tables_response, 'contents') and tables_response.contents:
                        tables_content = tables_response.contents
                        
                    if tables_content:
                        content = tables_content[0]
                        if hasattr(content, 'text'):
                            tables = json.loads(content.text)
                            
                            # For each table, get its columns
                            for table in tables:
                                table_name = table.get('table_name')
                                table_description = table.get('description', '')
                                
                                # Fetch columns for this table
                                columns_resource = f"pgmcp://{conn_id}/schemas/{schema_name}/tables/{table_name}/columns"
                                columns_response = await session.read_resource(columns_resource)
                                
                                columns = []
                                columns_content = None
                                if hasattr(columns_response, 'content') and columns_response.content:
                                    columns_content = columns_response.content
                                elif hasattr(columns_response, 'contents') and columns_response.contents:
                                    columns_content = columns_response.contents
                                    
                                if columns_content:
                                    content = columns_content[0]
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

async def generate_sql_with_anthropic(user_query, schema_text, anthropic_api_key):
    """Generate SQL using Claude with response template prefilling."""
    client = anthropic.Anthropic(api_key=anthropic_api_key)
    
    system_prompt = f"""You are an expert PostgreSQL developer who will translate a natural language query into a SQL query.
Before executing any query, first verify the table names and structure. 
If tables are missing, explain why the query cannot be executed.
You must provide your response in JSON format with two required fields:
1. "explanation": A brief explanation of your approach to the query
2. "sql": The valid, executable PostgreSQL SQL query

IMPORTANT: If your SQL query contains curly braces {{ or }}, you must escape them by doubling them: {{ becomes {{ and }} becomes }}.

Here is the database schema you will use:
{schema_text}
"""
    
    try:
        # Use response template prefilling to force Claude to produce JSON
        response = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1024,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_query},
                {"role": "assistant", "content": '{"explanation": "'}
            ]
        )
        
        # Extract the result
        result_text = response.content[0].text
        
        # First try to parse the complete JSON response
        try:
            if not result_text.strip().endswith('}'):
                result_text += '}'
            result_json = json.loads(result_text)
            
            # If we have both fields, process the SQL to handle escaped braces
            if "explanation" in result_json and "sql" in result_json:
                # Replace escaped braces in SQL
                sql = result_json["sql"].replace("{{", "{").replace("}}", "}")
                result_json["sql"] = sql
                return result_json
        except json.JSONDecodeError:
            # If parsing failed, try to extract fields manually
            pass
            
        # Manual extraction fallback
        explanation = ""
        sql = ""
        
        # Extract explanation
        if '"explanation":' in result_text:
            explanation_part = result_text.split('"explanation":', 1)[1].strip()
            if explanation_part.startswith('"'):
                explanation = explanation_part.split('"', 2)[1]
            else:
                explanation = explanation_part.split(',', 1)[0].strip()
                if explanation.endswith('"'):
                    explanation = explanation[:-1]
        
        # Extract SQL
        if '"sql":' in result_text:
            sql_part = result_text.split('"sql":', 1)[1].strip()
            if sql_part.startswith('"'):
                sql = sql_part.split('"', 2)[1]
            else:
                sql = sql_part.split('}', 1)[0].strip()
                if sql.endswith('"'):
                    sql = sql[:-1]
            
            # Replace escaped braces in SQL
            sql = sql.replace("{{", "{").replace("}}", "}")
        
        return {
            "explanation": explanation.replace('{"explanation": "', ''),
            "sql": sql
        }
            
    except Exception as e:
        print(f"Error calling Anthropic API: {e}")
        import traceback
        print(traceback.format_exc())
        return {
            "explanation": f"Error: {str(e)}",
            "sql": ""
        }

async def main():
    # Load environment variables
    dotenv.load_dotenv()
    anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')
    db_url = os.getenv('DATABASE_URL')
    pg_mcp_url = os.getenv('PG_MCP_URL', 'http://localhost:8000/sse')
    
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    
    if not anthropic_api_key:
        print("ERROR: ANTHROPIC_API_KEY environment variable is not set.")
        sys.exit(1)
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python cli.py 'your natural language query'")
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
                
                # Use the connect tool to register the connection
                print("Registering connection with server...")
                try:
                    connect_result = await session.call_tool(
                        "connect", 
                        {
                            "connection_string": db_url
                        }
                    )
                    
                    # Extract connection ID
                    if hasattr(connect_result, 'content') and connect_result.content:
                        content = connect_result.content[0]
                        if hasattr(content, 'text'):
                            result_data = json.loads(content.text)
                            conn_id = result_data.get('conn_id')
                            print(f"Connection registered with ID: {conn_id}")
                        else:
                            print("Error: Connection response missing text content")
                            sys.exit(1)
                    else:
                        print("Error: Connection response missing content")
                        sys.exit(1)
                except Exception as e:
                    print(f"Error registering connection: {e}")
                    sys.exit(1)
                
                # Fetch schema information
                print("Fetching database schema information...")
                schema_info = await fetch_schema_info(session, conn_id)
                schema_text = format_schema_for_prompt(schema_info)
                
                print(f"Retrieved information for {len(schema_info)} tables.")
                
                # Generate SQL using Claude with schema context
                print("Generating SQL query with Claude...")
                response_data = await generate_sql_with_anthropic(user_query, schema_text, anthropic_api_key)
                
                # Extract SQL and explanation
                sql_query = response_data.get("sql", "")
                explanation = response_data.get("explanation", "")
                
                # Print the results
                if explanation:
                    print(f"\nExplanation:")
                    print(f"------------")
                    print(explanation)
                
                print(f"\nGenerated SQL query:")
                print(f"------------------")
                print(sql_query)
                print(f"------------------\n")
                
                if not sql_query:
                    print("No SQL query was generated. Exiting.")
                    sys.exit(1)
                
                # Execute the generated SQL query
                sql_query = codecs.decode(sql_query, 'unicode_escape')
                print("Executing SQL query...")
                try:
                    result = await session.call_tool(
                        "pg_query", 
                        {
                            "query": sql_query,
                            "conn_id": conn_id
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
                    print(f"Failed query was: {sql_query}")
                
                # Disconnect when done
                print("Disconnecting from database...")
                try:
                    await session.call_tool(
                        "disconnect", 
                        {
                            "conn_id": conn_id
                        }
                    )
                    print("Successfully disconnected.")
                except Exception as e:
                    print(f"Error during disconnect: {e}")
                
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())