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
from dotenv import load_dotenv
load_dotenv()

class PostgreSQLAssistantApp:
    def __init__(self):
        st.set_page_config(
            page_title="Enterprise PostgreSQL Assistant",
            page_icon="🔍",
            layout="wide",
            initial_sidebar_state="expanded"
        )

        if 'messages' not in st.session_state:
            st.session_state.messages = []

        if 'selectbox_keys' not in st.session_state:
            st.session_state.selectbox_keys = set()

        if 'last_query_result' not in st.session_state:
            st.session_state.last_query_result = ""
            
        if 'last_query_df' not in st.session_state:
            st.session_state.last_query_df = None

        if 'sql_finished' not in st.session_state:
            st.session_state.sql_finished = False
            
        if 'conn_id' not in st.session_state:
            st.session_state.conn_id = None
            
        if 'query_history' not in st.session_state:
            st.session_state.query_history = []
            
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        self.anthropic_client = anthropic.AsyncAnthropic()
        self.pg_mcp_url = os.getenv("PG_MCP_URL", "http://localhost:8000/sse")
        self.db_url = os.getenv("DATABASE_URL", "")

    def get_unique_key(self, prefix=''):
        while True:
            key = f"{prefix}_{uuid.uuid4().hex[:8]}"
            if key not in st.session_state.selectbox_keys:
                st.session_state.selectbox_keys.add(key)
                return key

    def render_header(self):
        st.markdown("""
        # 📂 Enterprise Supply-GPT
        ### Advanced Data Query & Insights Platform
        """)
        st.divider()

    def render_sidebar(self):
        with st.sidebar:
            st.image("OPSVEDA-logo.png", use_container_width=True) 
            st.header("🛠️ Query Configuration")

            model_key = self.get_unique_key('model')
            model = st.selectbox(
                "Select AI Model", 
                ["claude-3-7-sonnet-latest"],
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
            
            # Add connection status indicator
            if st.session_state.conn_id:
                st.success(f"Connected (ID: {st.session_state.conn_id[:8]}...)")
            else:
                st.warning("Not connected to database")

        return model, max_tokens

    async def fetch_schema_info(self, session, conn_id):
        """Fetch database schema information from the MCP server."""
        schema_info = []
        
        if not conn_id:
            self.logger.warning("No connection ID available for fetching schema")
            return schema_info
            
        try:
            # First get all schemas
            schemas_resource = f"pgmcp://{conn_id}/schemas"
            self.logger.info(f"Fetching schemas from: {schemas_resource}")
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
            self.logger.error(f"Error fetching schema information: {e}")
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

    async def generate_visualizations(self, model):
        self.logger.info('Generating visualizations')
        result_text = st.session_state.last_query_result
        
        # Check if we have a dataframe from the query results
        has_df = hasattr(st.session_state, 'last_query_df') and isinstance(st.session_state.last_query_df, pd.DataFrame)
        
        # If neither text nor dataframe, exit
        if not result_text and not has_df:
            st.warning("No data available for visualization.")
            return
            
        # Show DataFrame info if available
        if has_df:
            df = st.session_state.last_query_df
            st.write("Data summary:")
            st.write(f"- {len(df)} rows, {len(df.columns)} columns")
            st.write("- Columns: " + ", ".join(df.columns.tolist()))
            
            # Auto-generate simple visualizations based on dataframe content
            if len(df) > 0 and len(df.columns) > 0:
                try:
                    # For numeric columns
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if len(numeric_cols) >= 1:
                        st.write("### Quick data overview")
                        st.bar_chart(df.iloc[:10, df.columns.get_indexer(numeric_cols[:2])])
                except Exception as e:
                    self.logger.error(f"Error in auto-visualization: {e}")
        
        system_prompt = """
        You are a data visualization expert. You will receive the result of a SQL query.
        Your task is to propose meaningful visualizations using plotly based on the data.
        Only return Python code that creates the visualizations. The code should handle
        potential empty datasets gracefully and include comments explaining your choices.
        
        Never invent or hallucinate data. If there's no useful data to visualize, suggest
        appropriate code that checks for data validity before attempting visualization.
        """
        
        # Prepare message content based on available data
        message_content = f"Here is the SQL query result:\n\n{result_text}\n\n"
        if has_df:
            # Add DataFrame description
            message_content += f"DataFrame info:\n"
            message_content += f"Shape: {st.session_state.last_query_df.shape}\n"
            message_content += f"Columns: {st.session_state.last_query_df.columns.tolist()}\n"
            message_content += f"Data types: {st.session_state.last_query_df.dtypes.to_dict()}\n"
            # Add a few rows as examples
            if len(st.session_state.last_query_df) > 0:
                message_content += f"First few rows:\n{st.session_state.last_query_df.head(3).to_string()}\n"
        
        message_content += "\nPlease generate visualizations if appropriate."
        
        messages = [
            {"role": "user", "content": message_content}
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
                        self.logger.error(f"Visualization execution error: {exec_err}")

        except Exception as e:
            st.warning(f"Could not generate visualizations: {e}")
            self.logger.error(f"Visualization generation error: {e}")

    async def establish_connection(self, session):
        """Establish a connection to the PostgreSQL database."""
        if not self.db_url:
            st.error("Database URL is not set. Please check your environment variables.")
            return False
            
        try:
            with st.spinner("Connecting to database..."):
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
                        new_conn_id = result_data.get('conn_id')
                        
                        if new_conn_id:
                            st.session_state.conn_id = new_conn_id
                            self.logger.info(f"Connected to PostgreSQL database with ID: {new_conn_id}")
                            return True
                        else:
                            st.error("Error: Connection response missing conn_id")
                            return False
                    else:
                        st.error("Error: Connection response missing text content")
                        return False
                else:
                    st.error("Error: Connection response missing content")
                    return False
        except Exception as e:
            st.error(f"Error connecting to PostgreSQL: {e}")
            self.logger.error(f"Database connection error: {e}")
            return False

    async def process_query(self, session, query, model, max_tokens):
        try:
            # First, ensure we have a connection
            if not st.session_state.conn_id:
                connection_success = await self.establish_connection(session)
                if not connection_success:
                    return

            # Fetch schema information
            schema_info = await self.fetch_schema_info(session, st.session_state.conn_id)
            schema_text = self.format_schema_for_prompt(schema_info)

            messages = st.session_state.messages + [
                {"role": "user", "content": query}
            ]

            response = await session.list_tools()
            available_tools = [
                {
                    "name": tool.name,
                    "description": tool.description or "",
                    "input_schema": tool.inputSchema,
                }
                for tool in response.tools
            ]

            system_prompt = textwrap.dedent(f"""\
                You are a master PostgreSQL assistant. 
                Before executing any query, first verify the table names and structure. 
                If tables are missing, explain why the query cannot be executed. 
                
                IMPORTANT: Never make up or hallucinate data. Only discuss the actual results returned by the SQL query.
                If the query returns no results or an empty table, clearly state this fact. Do not invent sample data.
                Always base your analysis strictly on the data returned by the executed SQL queries.
                
                When using the pg_query tool, always include the conn_id parameter with the value: {st.session_state.conn_id}
                
                {schema_text}
            """)

            while True:
                ai_response = await self.anthropic_client.messages.create(
                    model=model,
                    system=system_prompt,
                    max_tokens=max_tokens,
                    messages=messages,
                    tools=available_tools
                )

                assistant_message_content: List[Dict[str, Any]] = []
                tool_uses = []

                for content in ai_response.content:
                    if content.type == "text":
                        assistant_message_content.append({"type": "text", "text": content.text})
                        st.chat_message("assistant").write(content.text)
                        st.session_state.last_query_result = content.text
                    elif content.type == "tool_use":
                        tool_uses.append(content)
                        assistant_message_content.append({
                            "type": "tool_use", 
                            "id": content.id, 
                            "name": content.name, 
                            "input": content.input
                        })

                messages.append({
                    "role": "assistant",
                    "content": assistant_message_content
                })

                if not tool_uses:
                    break

                tool_results = []
                for i, tool_use in enumerate(tool_uses):
                    try:
                        # If the tool is pg_query, make sure it has the conn_id
                        if tool_use.name == "pg_query":
                            tool_input = tool_use.input
                            # Log the original tool input for debugging
                            self.logger.info(f"Original pg_query input: {tool_input}")
                            
                            # Make a copy to avoid modifying the original
                            if isinstance(tool_input, dict):
                                # Create a new dict with the conn_id added
                                updated_input = dict(tool_input)
                                updated_input["conn_id"] = st.session_state.conn_id
                                self.logger.info(f"Updated pg_query input: {updated_input}")
                                
                                # Call the tool with the updated input
                                result = await session.call_tool(
                                    tool_use.name, 
                                    updated_input
                                )
                            else:
                                # If input is not a dict, log this error case
                                self.logger.error(f"pg_query tool input is not a dict: {type(tool_input)}")
                                # Try to convert to dict if possible
                                if isinstance(tool_input, str):
                                    try:
                                        updated_input = json.loads(tool_input)
                                        updated_input["conn_id"] = st.session_state.conn_id
                                        result = await session.call_tool(
                                            tool_use.name, 
                                            updated_input
                                        )
                                    except:
                                        st.error("Invalid tool input format for pg_query")
                                        continue
                                else:
                                    st.error("Invalid tool input format for pg_query")
                                    continue
                        else:
                            # For other tools, call normally
                            result = await session.call_tool(
                                tool_use.name, 
                                cast(dict, tool_use.input)
                            )

                        # Process the result
                        if hasattr(result, 'content') and result.content and len(result.content) > 0:
                            content_item = result.content[0]
                            if hasattr(content_item, "text"):
                                result_text = content_item.text.strip().replace('\x00', '')
                            else:
                                result_text = f"Tool returned content without text attribute: {content_item}"
                        else:
                            result_text = "Tool returned no content"
                        
                        # Display query and results in separate expandable sections
                        if tool_use.name == "pg_query":
                            # Display the SQL query
                            with st.expander("📜 Executed SQL Query", expanded=True):
                                try:
                                    sql_display = tool_use.input.get("query") if isinstance(tool_use.input, dict) else str(tool_use.input)
                                    if sql_display:
                                        st.code(sql_display, language='sql')
                                    else:
                                        st.write("Raw tool input:", tool_use.input)
                                except Exception as e:
                                    st.warning(f"Failed to retrieve tool input: {e}")
                                    st.write("Raw tool input:", tool_use.input)
                            
                            # Display the query results in a more structured way
                            with st.expander("📊 SQL Query Results", expanded=True):
                                try:
                                    # Try to parse result as JSON for better display
                                    try:
                                        json_result = json.loads(result_text)
                                        if isinstance(json_result, list) and len(json_result) > 0:
                                            # Convert to DataFrame for display
                                            df = pd.DataFrame(json_result)
                                            st.dataframe(df, use_container_width=True)
                                            # Store the dataframe for visualizations
                                            st.session_state.last_query_df = df
                                        elif isinstance(json_result, dict):
                                            st.json(json_result)
                                        else:
                                            st.text(result_text)
                                    except json.JSONDecodeError:
                                        # If not JSON, check if it's CSV-like
                                        try:
                                            if ',' in result_text and '\n' in result_text:
                                                df = pd.read_csv(io.StringIO(result_text))
                                                st.dataframe(df, use_container_width=True)
                                                # Store the dataframe for visualizations
                                                st.session_state.last_query_df = df
                                            else:
                                                st.text(result_text)
                                        except:
                                            st.text(result_text)
                                except Exception as display_err:
                                    st.error(f"Error displaying results: {display_err}")
                                    st.text(result_text)
                        else:
                            # For non-SQL tools, display as before
                            with st.expander(f"🔧 Tool: {tool_use.name}", expanded=True):
                                st.write("Input:", tool_use.input)
                                st.text(result_text)

                        tool_result = {
                            "type": "tool_result",
                            "tool_use_id": tool_use.id,
                            "content": result_text
                        }
                        tool_results.append(tool_result)

                    except Exception as tool_error:
                        self.logger.error(f"Tool execution error: {tool_error}")
                        st.error(f"Tool execution error: {tool_error}")
                        
                        # Add the error as a tool result to inform the AI
                        tool_result = {
                            "type": "tool_result",
                            "tool_use_id": tool_use.id,
                            "content": f"Error: {str(tool_error)}"
                        }
                        tool_results.append(tool_result)

                messages.append({
                    "role": "user",
                    "content": tool_results
                })

            st.session_state.sql_finished = True

        except Exception as e:
            st.error(f"Query processing error: {e}")
            self.logger.error(f"Query processing error: {e}")

    async def run_app(self):
        # Connect to MCP server
        async with sse_client(url=self.pg_mcp_url) as streams:
            # Create and initialize the MCP ClientSession
            async with ClientSession(*streams) as session:
                await session.initialize()
                
                # Process user query if present
                query = st.session_state.get("current_query", None)
                if query:
                    model = st.session_state.get("model", "claude-3-7-sonnet-latest")
                    max_tokens = st.session_state.get("max_tokens", 8000)
                    await self.process_query(session, query, model, max_tokens)
                    st.session_state.current_query = None
                
                # Run visualization if SQL query finished
                if st.session_state.get("sql_finished"):
                    with st.expander("📊 AI-Generated Visualizations"):
                        await self.generate_visualizations(st.session_state.get("model", "claude-3-7-sonnet-latest"))
                        st.session_state.sql_finished = False
                
                # Keep the connection alive for the next query
                # Only disconnect when the app is stopping or when explicitly requested

    def run(self):
        self.render_header()
        model, max_tokens = self.render_sidebar()

        # Create tabs for different sections
        tab1, tab2 = st.tabs(["Chat Interface", "Query History"])
        
        with tab1:
            query = st.chat_input("Enter your SQL query...")

            # Display disconnect button if connected
            if st.session_state.conn_id:
                if st.sidebar.button("Disconnect from Database"):
                    st.session_state.conn_id = None
                    st.sidebar.success("Disconnected from database")
                    st.rerun()

            # Show conversation history
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    if isinstance(message["content"], list):
                        for content in message["content"]:
                            if content.get("type") == "text":
                                st.write(content["text"])
                    else:
                        st.write(message["content"])

            if query:
                st.session_state.sql_finished = False
                st.session_state.model = model
                st.session_state.max_tokens = max_tokens
                st.session_state.current_query = query
                
                # Store the query in history
                timestamp = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                st.session_state.query_history.append({
                    "timestamp": timestamp,
                    "query": query
                })
                
                # Store the message
                st.session_state.messages.append({"role": "user", "content": query})
                
                with st.chat_message("user"):
                    st.write(query)

                asyncio.run(self.run_app())
        
        with tab2:
            # Display query history
            if st.session_state.query_history:
                st.write("### Recent SQL Queries")
                for i, hist_item in enumerate(reversed(st.session_state.query_history[-10:])):
                    with st.expander(f"{hist_item['timestamp']} - Query {len(st.session_state.query_history)-i}", expanded=False):
                        st.code(hist_item['query'], language='sql')
                        if st.button(f"Run Again", key=f"rerun_{i}"):
                            st.session_state.current_query = hist_item['query']
                            st.rerun()
            else:
                st.info("No query history yet. Try running some SQL queries!")
                
            # Add a clear history button
            if st.session_state.query_history and st.button("Clear History"):
                st.session_state.query_history = []
                st.success("Query history cleared!")
                st.rerun()

def main():
    app = PostgreSQLAssistantApp()
    app.run()

if __name__ == "__main__":
    main()