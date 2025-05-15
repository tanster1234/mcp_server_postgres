# server/resources/schema.py
from server.config import mcp
from mcp.server.fastmcp.utilities.logging import get_logger
from server.tools.query import execute_query

logger = get_logger("pg-mcp.resources.schemas")

def register_schema_resources():
    """Register database schema resources with the MCP server."""
    logger.debug("Registering schema resources")
    
    @mcp.resource("pgmcp://{conn_id}/schemas")
    async def list_schemas(conn_id: str):
        """List all non-system schemas in the database."""
        query = """
            SELECT 
                schema_name,
                obj_description(pg_namespace.oid) as description
            FROM information_schema.schemata
            JOIN pg_namespace ON pg_namespace.nspname = schema_name
            WHERE 
                schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND schema_name NOT LIKE 'pg_%'
            ORDER BY schema_name
        """
        return await execute_query(query, conn_id)
    
    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables")
    async def list_schema_tables(conn_id: str, schema: str):
        """List all tables in a specific schema with their descriptions."""
        query = """
            SELECT 
                t.table_name,
                obj_description(format('"%s"."%s"', t.table_schema, t.table_name)::regclass::oid) as description,
                pg_stat_get_tuples_inserted(format('"%s"."%s"', t.table_schema, t.table_name)::regclass::oid) as total_rows
            FROM information_schema.tables t
            WHERE 
                t.table_schema = $1
                AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """
        return await execute_query(query, conn_id, [schema])
    
    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/select_tables") 
    async def list_select_schema_tables(conn_id: str, schema: str): 
        """List tables from a specified context table with their table descriptions.""" 
        query = """ 
                SELECT t.* FROM public.context_schema_table t
        """ 
        return await execute_query(query, conn_id)



    
    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables/{table}/columns")
    async def get_table_columns(conn_id: str, schema: str, table: str):
        """Get columns for a specific table with their descriptions."""
        query = """
            SELECT
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) as description
            FROM information_schema.columns c
            WHERE
                c.table_schema = $1 AND
                c.table_name = $2
            ORDER BY c.ordinal_position
        """
        return await execute_query(query, conn_id, [schema, table])
        
    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables/{table}/indexes")
    async def get_table_indexes(conn_id: str, schema: str, table: str):
        """Get indexes for a specific table with their descriptions."""
        query = """
            SELECT 
                i.relname as index_name,
                pg_get_indexdef(i.oid) as index_definition,
                obj_description(i.oid) as description,
                am.amname as index_type,
                ARRAY_AGG(a.attname ORDER BY k.i) as column_names,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                ix.indisexclusion as is_exclusion
            FROM 
                pg_index ix
            JOIN 
                pg_class i ON i.oid = ix.indexrelid
            JOIN 
                pg_class t ON t.oid = ix.indrelid
            JOIN 
                pg_namespace n ON n.oid = t.relnamespace
            JOIN 
                pg_am am ON i.relam = am.oid
            LEFT JOIN 
                LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, i) ON TRUE
            LEFT JOIN 
                pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE 
                n.nspname = $1
                AND t.relname = $2
            GROUP BY
                i.relname, i.oid, am.amname, ix.indisunique, ix.indisprimary, ix.indisexclusion
            ORDER BY 
                i.relname
        """
        return await execute_query(query, conn_id, [schema, table])

    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables/{table}/constraints")
    async def get_table_constraints(conn_id: str, schema: str, table: str):
        """Get constraints for a specific table with their descriptions."""
        query = """
            SELECT 
                c.conname as constraint_name,
                c.contype as constraint_type,
                CASE 
                    WHEN c.contype = 'p' THEN 'PRIMARY KEY'
                    WHEN c.contype = 'u' THEN 'UNIQUE'
                    WHEN c.contype = 'f' THEN 'FOREIGN KEY'
                    WHEN c.contype = 'c' THEN 'CHECK'
                    WHEN c.contype = 't' THEN 'TRIGGER'
                    WHEN c.contype = 'x' THEN 'EXCLUSION'
                    ELSE 'OTHER'
                END as constraint_type_desc,
                obj_description(c.oid) as description,
                pg_get_constraintdef(c.oid) as definition,
                CASE 
                    WHEN c.contype = 'f' THEN 
                        (SELECT nspname FROM pg_namespace WHERE oid = ref_table.relnamespace) || '.' || ref_table.relname
                    ELSE NULL
                END as referenced_table,
                ARRAY_AGG(col.attname ORDER BY u.attposition) as column_names
            FROM 
                pg_constraint c
            JOIN 
                pg_namespace n ON n.oid = c.connamespace
            JOIN 
                pg_class t ON t.oid = c.conrelid
            LEFT JOIN 
                pg_class ref_table ON ref_table.oid = c.confrelid
            LEFT JOIN 
                LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
            LEFT JOIN 
                pg_attribute col ON col.attrelid = t.oid AND col.attnum = u.attnum
            WHERE 
                n.nspname = $1
                AND t.relname = $2
            GROUP BY
                c.conname, c.contype, c.oid, ref_table.relname, ref_table.relnamespace
            ORDER BY 
                c.contype, c.conname
        """
        return await execute_query(query, conn_id, [schema, table])

    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables/{table}/indexes/{index}")
    async def get_index_details(conn_id: str, schema: str, table: str, index: str):
        """Get detailed information about a specific index."""
        query = """
            SELECT 
                i.relname as index_name,
                pg_get_indexdef(i.oid) as index_definition,
                obj_description(i.oid) as description,
                am.amname as index_type,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary,
                ix.indisexclusion as is_exclusion,
                ix.indimmediate as is_immediate,
                ix.indisclustered as is_clustered,
                ix.indisvalid as is_valid,
                i.relpages as pages,
                i.reltuples as rows,
                ARRAY_AGG(a.attname ORDER BY k.i) as column_names,
                ARRAY_AGG(pg_get_indexdef(i.oid, k.i, false) ORDER BY k.i) as column_expressions
            FROM 
                pg_index ix
            JOIN 
                pg_class i ON i.oid = ix.indexrelid
            JOIN 
                pg_class t ON t.oid = ix.indrelid
            JOIN 
                pg_namespace n ON n.oid = t.relnamespace
            JOIN 
                pg_am am ON i.relam = am.oid
            LEFT JOIN 
                LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, i) ON TRUE
            LEFT JOIN 
                pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE 
                n.nspname = $1
                AND t.relname = $2
                AND i.relname = $3
            GROUP BY
                i.relname, i.oid, am.amname, ix.indisunique, ix.indisprimary, 
                ix.indisexclusion, ix.indimmediate, ix.indisclustered, ix.indisvalid,
                i.relpages, i.reltuples
        """
        return await execute_query(query, conn_id, [schema, table, index])

    @mcp.resource("pgmcp://{conn_id}/schemas/{schema}/tables/{table}/constraints/{constraint}")
    async def get_constraint_details(conn_id: str, schema: str, table: str, constraint: str):
        """Get detailed information about a specific constraint."""
        query = """
            SELECT 
                c.conname as constraint_name,
                c.contype as constraint_type,
                CASE 
                    WHEN c.contype = 'p' THEN 'PRIMARY KEY'
                    WHEN c.contype = 'u' THEN 'UNIQUE'
                    WHEN c.contype = 'f' THEN 'FOREIGN KEY'
                    WHEN c.contype = 'c' THEN 'CHECK'
                    WHEN c.contype = 't' THEN 'TRIGGER'
                    WHEN c.contype = 'x' THEN 'EXCLUSION'
                    ELSE 'OTHER'
                END as constraint_type_desc,
                obj_description(c.oid) as description,
                pg_get_constraintdef(c.oid) as definition,
                CASE 
                    WHEN c.contype = 'f' THEN 
                        (SELECT nspname FROM pg_namespace WHERE oid = ref_table.relnamespace) || '.' || ref_table.relname
                    ELSE NULL
                END as referenced_table,
                ARRAY_AGG(col.attname ORDER BY u.attposition) as column_names,
                CASE 
                    WHEN c.contype = 'f' THEN 
                        ARRAY_AGG(ref_col.attname ORDER BY u2.attposition)
                    ELSE NULL
                END as referenced_columns
            FROM 
                pg_constraint c
            JOIN 
                pg_namespace n ON n.oid = c.connamespace
            JOIN 
                pg_class t ON t.oid = c.conrelid
            LEFT JOIN 
                pg_class ref_table ON ref_table.oid = c.confrelid
            LEFT JOIN 
                LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, attposition) ON TRUE
            LEFT JOIN 
                pg_attribute col ON col.attrelid = t.oid AND col.attnum = u.attnum
            LEFT JOIN 
                LATERAL unnest(c.confkey) WITH ORDINALITY AS u2(attnum, attposition) ON c.contype = 'f'
            LEFT JOIN 
                pg_attribute ref_col ON c.contype = 'f' AND ref_col.attrelid = c.confrelid AND ref_col.attnum = u2.attnum
            WHERE 
                n.nspname = $1
                AND t.relname = $2
                AND c.conname = $3
            GROUP BY
                c.conname, c.contype, c.oid, ref_table.relname, ref_table.relnamespace
        """
        return await execute_query(query, conn_id, [schema, table, constraint])