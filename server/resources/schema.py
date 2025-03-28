# server/resources/schema.py
from server.config import mcp
from mcp.server.fastmcp.utilities.logging import get_logger
from server.tools.query import execute_query

logger = get_logger("pg-mcp.resources.schema")

def register_schema_resources():
    """Register database schema resources with the MCP server."""
    logger.debug("Registering schema resources")
    
    @mcp.resource("pgmcp://{conn_id}/tables")
    async def list_tables(conn_id: str):
        """List all tables in the database with their descriptions."""
        query = """
            SELECT 
                t.table_schema,
                t.table_name,
                obj_description(format('%s.%s', t.table_schema, t.table_name)::regclass::oid) as description
            FROM information_schema.tables t
            WHERE 
                t.table_schema NOT IN ('pg_catalog', 'information_schema') 
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_schema, t.table_name
        """
        return await execute_query(query, conn_id)
    
    @mcp.resource("pgmcp://{conn_id}/tables/{schema}/{table}/columns")
    async def get_table_columns(conn_id: str, schema: str, table: str):
        """Get columns for a specific table with their descriptions."""
        query ="""
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    pgd.description
                FROM pg_catalog.pg_statio_all_tables AS st
                INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid = st.relid)
                INNER JOIN information_schema.columns c ON (
                    pgd.objsubid = c.ordinal_position AND
                    c.table_schema = st.schemaname AND
                    c.table_name = st.relname
                )
                WHERE
                    c.table_schema = $1 AND
                    c.table_name = $2
                ORDER BY c.ordinal_position
            """
        return await execute_query(query, conn_id, [schema, table])