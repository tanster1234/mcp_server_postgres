import streamlit as st
import asyncio
import anthropic
import textwrap
from mcp import ClientSession
from mcp.client.sse import sse_client
from typing import Union, cast, List, Dict, Any
import pandas as pd
import io
import logging
import uuid
import json
import re
import plotly.express as px
import plotly.graph_objects as go
import os
import dotenv

class PostgreSQLAssistantApp:
    def __init__(self):
        st.set_page_config(
            page_title="Enterprise PostgreSQL Assistant",
            page_icon="🔍",
            layout="wide",
            initial_sidebar_state="expanded"
        )

        # Load environment variables
        dotenv.load_dotenv()
        self.db_url = os.getenv('DATABASE_URL')
        self.pg_mcp_url = os.getenv('PG_MCP_URL', 'http://localhost:8000/sse')
        self.anthropic_api_key = os.getenv('ANTHROPIC_API_KEY')

        if not self.anthropic_api_key:
            st.error("ANTHROPIC_API_KEY environment variable is not set.")
            st.stop()

        if 'messages' not in st.session_state:
            st.session_state.messages = []

        if 'selectbox_keys' not in st.session_state:
            st.session_state.selectbox_keys = set()

        if 'last_query_result' not in st.session_state:
            st.session_state.last_query_result = ""

        if 'sql_finished' not in st.session_state:
            st.session_state.sql_finished = False

        if 'conn_id' not in st.session_state:
            st.session_state.conn_id = None

        if 'schema_info' not in st.session_state:
            st.session_state.schema_info = []

        self.anthropic_client = anthropic.AsyncAnthropic(
            api_key=self.anthropic_api_key
        )

    async def fetch_schema_info(self, session, conn_id):
        """Fetch database schema information from the MCP server."""
        schema_info = []
        
        try:
            # First get all schemas
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
            st.error(f"Error fetching schema information: {e}")
            logging.error(f"Error fetching schema information: {e}")
            return []

    def format_schema_for_prompt(self, schema_info):
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

    def get_unique_key(self, prefix=''):
        while True:
            key = f"{prefix}_{uuid.uuid4().hex[:8]}"
            if key not in st.session_state.selectbox_keys:
                st.session_state.selectbox_keys.add(key)
                return key

    def render_header(self):
        st.markdown("""
        # 📂 Enterprise PostgreSQL Intelligence
        ### Advanced Data Query & Insights Platform
        """)
        st.divider()

    def render_sidebar(self):
        with st.sidebar:
            st.header("🛠️ Query Configuration")

            # Database URL input
            if not self.db_url:
                self.db_url = st.text_input("PostgreSQL Connection URL", 
                                         placeholder="postgresql://user:password@host:port/dbname",
                                         type="password")

            model_key = self.get_unique_key('model')
            model = st.selectbox(
                "Select AI Model", 
                ["claude-3-7-sonnet-latest", "claude-3-5-sonnet-20240620"],
                key=model_key
            )

            tokens_key = self.get_unique_key('tokens')
            max_tokens = st.slider(
                "Max Response Tokens", 
                min_value=1000, 
                max_value=16000, 
                value=8000,
                key=tokens_key
            )

            st.divider()
            st.info("""
            💡 Pro Tip:
            - Use clear, precise SQL queries
            - Check table names before querying
            - Leverage AI for complex data analysis
            """)

            # Display database connection status
            if st.session_state.conn_id:
                st.success(f"Connected to database")
            elif self.db_url:
                st.warning("Not connected to database")

        return model, max_tokens

    async def generate_visualizations(self, model):
        result_text = st.session_state.last_query_result
        if not result_text:
            st.warning("No final output available for visualization.")
            return

        system_prompt = """
        You are a data visualization expert. You will receive the result of a SQL query in plain text (not a DataFrame).
        It may contain insights or summaries, not necessarily tabular data. Your task is to propose meaningful and
        relevant visualizations using plotly based on the textual result. Only return Python code that creates the visualizations.
        """

        messages = [
            {"role": "user", "content": f"Here is the SQL query result:\n\n{result_text}\n\nPlease generate visualizations if appropriate."}
        ]

        try:
            response = await self.anthropic_client.messages.create(
                model=model,
                system=system_prompt,
                max_tokens=2000,
                messages=messages
            )

            for i, content in enumerate(response.content):
                if content.type == "text":
                    st.code(content.text, language='python')
                    try:
                        cleaned_code = re.sub(r'^```(?:python)?\n|```$', '', content.text.strip(), flags=re.MULTILINE).strip()
                        exec_globals = {
                            "st": st,
                            "pd": pd,
                            "px": px,
                            "go": go
                        }

                        # Patch timeline to avoid x_start == x_end error
                        original_timeline = px.timeline
                        def safe_timeline(*args, **kwargs):
                            df = kwargs.get('data_frame', args[0] if args else None)
                            if df is not None and 'x_start' in kwargs and 'x_end' in kwargs:
                                if isinstance(df, pd.DataFrame):
                                    df = df.copy()
                                    x_start = kwargs['x_start']
                                    x_end = kwargs['x_end']
                                    if (df[x_end] == df[x_start]).all():
                                        df[x_end] = pd.to_datetime(df[x_end]) + pd.Timedelta(days=1)
                                    kwargs['data_frame'] = df
                            fig = original_timeline(*args, **kwargs)
                            fig.show = lambda: st.plotly_chart(fig, use_container_width=True)
                            return fig
                        exec_globals['px'].timeline = safe_timeline

                        # Patch all show() methods to use Streamlit
                        def patched_show(fig):
                            st.plotly_chart(fig, use_container_width=True)
                        exec_globals['go'].Figure.show = patched_show
                        exec_globals['px'].pie().show = patched_show
                        exec_globals['px'].scatter().show = patched_show
                        exec_globals['px'].bar().show = patched_show
                        exec_globals['px'].line().show = patched_show

                        exec(cleaned_code, exec_globals)
                    except Exception as exec_err:
                        st.error(f"Execution error: {exec_err}")

        except Exception as e:
            st.warning(f"Could not generate visualizations: {e}")

    async def generate_sql_with_anthropic(self, user_query, schema_text, model, max_tokens):
        """Generate SQL using Claude with response template prefilling."""
        
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
            response = await self.anthropic_client.messages.create(
                model=model,
                max_tokens=max_tokens,
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
                    # Find the end of the SQL statement, looking for the last non-JSON closing brace
                    parts = sql_part.split('}')
                    if len(parts) > 1:
                        # If there are multiple closing braces, take everything except the last one
                        # as the last one is likely the JSON closing brace
                        sql = '}'.join(parts[:-1]).strip()
                    else:
                        sql = sql_part.strip()
                    if sql.endswith('"'):
                        sql = sql[:-1]
                
                # Replace escaped braces in SQL
                sql = sql.replace("{{", "{").replace("}}", "}")
            
            return {
                "explanation": explanation.replace('{"explanation": "', ''),
                "sql": sql
            }
                
        except Exception as e:
            st.error(f"Error calling Anthropic API: {e}")
            import traceback
            st.error(traceback.format_exc())
            return {
                "explanation": f"Error: {str(e)}",
                "sql": ""
            }

    async def process_query(self, session, query, model, max_tokens):
        try:
            # Add user message to chat history
            with st.chat_message("user"):
                st.write(query)
                
            st.session_state.messages.append({"role": "user", "content": query})
            
            # Get schema information for the prompt
            schema_text = self.format_schema_for_prompt(st.session_state.schema_info)
            
            # First, generate SQL using Claude
            with st.status("Generating SQL query..."):
                response_data = await self.generate_sql_with_anthropic(query, schema_text, model, max_tokens)
                
            # Extract SQL and explanation
            sql_query = response_data.get("sql", "")
            explanation = response_data.get("explanation", "")
            
            # Show the explanation and generated SQL
            with st.chat_message("assistant"):
                if explanation:
                    st.write(explanation)
                
                if sql_query:
                    with st.expander("Generated SQL Query"):
                        st.code(sql_query, language="sql")
                else:
                    st.error("No SQL query was generated.")
                    return
            
            # Execute the SQL query
            if sql_query and st.session_state.conn_id:
                with st.status("Executing SQL query..."):
                    try:
                        result = await session.call_tool(
                            "pg_query", 
                            {
                                "query": sql_query,
                                "conn_id": st.session_state.conn_id
                            }
                        )
                        
                        # Extract and format results
                        if hasattr(result, 'content') and result.content:
                            content = result.content[0]
                            if hasattr(content, 'text'):
                                query_results = json.loads(content.text)
                                
                                # Display results as a table
                                with st.chat_message("assistant"):
                                    st.write("Query Results:")
                                    if query_results and isinstance(query_results, list):
                                        df = pd.DataFrame(query_results)
                                        st.dataframe(df)
                                        st.write(f"Total rows: {len(query_results)}")
                                        
                                        # Store result for visualization
                                        result_summary = f"Query returned {len(query_results)} rows.\n\n"
                                        result_summary += df.head(20).to_string()
                                        st.session_state.last_query_result = result_summary
                                    elif query_results:
                                        st.json(query_results)
                                        st.session_state.last_query_result = json.dumps(query_results, indent=2)
                                    else:
                                        st.info("Query executed successfully but returned no results.")
                                        st.session_state.last_query_result = "Query executed but no results returned."
                            else:
                                st.warning("Query executed but returned an unexpected format.")
                        else:
                            st.warning("Query executed but returned no content.")
                    except Exception as e:
                        st.error(f"Error executing SQL query: {e}")
                        st.error(f"Failed query was: {sql_query}")
            
            st.session_state.sql_finished = True
            
        except Exception as e:
            st.error(f"Query processing error: {e}")
            logging.error(f"Query processing error: {e}")

    async def connect_to_database(self, session):
        """Connect to the PostgreSQL database and store connection ID"""
        if not self.db_url:
            st.error("Database URL not provided. Please configure it in the sidebar.")
            return False
        
        try:
            # Use the connect tool to register the connection
            connect_result = await session.call_tool(
                "connect",
                {
                    "connection_string": self.db_url
                }
            )
            
            # Extract connection ID
            if hasattr(connect_result, 'content') and connect_result.content:
                content = connect_result.content[0]
                if hasattr(content, 'text'):
                    result_data = json.loads(content.text)
                    conn_id = result_data.get('conn_id')
                    st.session_state.conn_id = conn_id
                    
                    # Fetch schema information
                    with st.status("Fetching database schema..."):
                        st.session_state.schema_info = await self.fetch_schema_info(session, conn_id)
                    
                    return True
            
            st.error("Failed to connect to database: Invalid response from server")
            return False
            
        except Exception as e:
            st.error(f"Failed to connect to database: {e}")
            logging.error(f"Database connection error: {e}")
            return False

    async def run_async(self):
        model, max_tokens = self.render_sidebar()
        
        # Use SSE transport for PostgreSQL
        async with sse_client(url=self.pg_mcp_url) as streams:
            async with ClientSession(*streams) as session:
                await session.initialize()
                
                # Connect to database if not already connected
                if not st.session_state.conn_id:
                    if not await self.connect_to_database(session):
                        return
                
                # Process user query
                query = st.chat_input("Enter your natural language query...")
                
                if query:
                    st.session_state.sql_finished = False
                    await self.process_query(
                        session, 
                        query, 
                        model=model, 
                        max_tokens=max_tokens
                    )
                
                if st.session_state.get("sql_finished"):
                    with st.expander("📊 AI-Generated Visualizations"):
                        await self.generate_visualizations(model)
                        st.session_state.sql_finished = False

    def run(self):
        self.render_header()
        asyncio.run(self.run_async())

def main():
    app = PostgreSQLAssistantApp()
    app.run()

if __name__ == "__main__":
    main()