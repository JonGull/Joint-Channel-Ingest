import pyodbc
import re
import pandas as pd


def get_table_names(search_regex = None):
    """Retrieve a list of tables from the kcom database.

    Args:
        search_regex (string, optional): Enter a search term to filter down the list of tables(really a regular expression). Defaults to None.

    Returns:
        list: A list of the tables in the kcom database.
    """
    conn, cursor = _get_kcom_conn_and_cursor()
    show_tables = cursor.tables()
    table_list = []
    for _t in show_tables:
        if str(_t.table_type) == 'TABLE':
            table_list += [_t.table_schem + '.' +_t.table_name]
    _close_kcom_conn_and_cursor(conn, cursor)

    if search_regex is not None:
        table_list = [ x for x in get_table_names() if re.search(search_regex, x, flags=re.I)]
    return table_list

# Dump a table to a pandas dataframe. Defaults to dbo as its prefix.
def get_table_as_dataframe(table, table_prefix = 'dbo', header=True, database = 'liveengage_chat_data'):
    conn, cursor = _get_kcom_conn_and_cursor(database=database)
    sql_query = pd.read_sql_query(f'SELECT * FROM {table_prefix}.{table}',conn, coerce_float=False)
    _close_kcom_conn_and_cursor(conn, cursor)
    return sql_query

def get_table_as_csv(table, table_prefix = 'dbo', header=True):
    """Load a SQL table and write it to a csv.

    Args:
        table (str): Name of the table you're trying to load.
        table_prefix (str, optional): [Prefix of the table you're trying to load, if not dbo.]. Defaults to 'dbo'.
        header (str, optional): [Whether you want the table to have headers.]. Defaults to 'True'.
        path (str, optional): [Path you're trying to write to. Writes to a temp path in the project folder otherwise.]. Defaults to './table_dump_temp.csv'.
    """
    _df = get_table_as_dataframe(table_prefix=table_prefix)
    _df.to_csv(f'{table_prefix}{table}', sep=',', header=header, index=0, na_rep='NULL')


def get_query_as_dataframe(query, database = 'liveengage_chat_data', tableau_server = False):
    dangerous_queries = ['delete', 'drop', 'update', 'insert']
    for q in dangerous_queries:
        assert q not in query.lower(), f'Avoid modification queries such as {q} with this function.'
    conn, cursor = _get_kcom_conn_and_cursor(database=database, tableau_server=tableau_server)
    sql_query = pd.read_sql_query(query,conn, coerce_float=False)
    _close_kcom_conn_and_cursor(conn, cursor)
    return sql_query

def insert_dataframe_into_table(dataframe, table, table_prefix= 'dbo'):
    conn, cursor = _get_kcom_conn_and_cursor()
    sql_insert = dataframe.to_sql(table, conn)
    _close_kcom_conn_and_cursor(conn, cursor)
    return sql_insert


# "Internals"


# Get a connection and cursor.
def _get_kcom_conn_and_cursor(database = 'liveengage_chat_data', tableau_server = False):
    if tableau_server == False:
        conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=CAAPTSFARMBI02;'
                        f'Database={database};'
                        'Trusted_Connection=yes;')
    elif tableau_server == True:
        conn = pyodbc.connect('Driver={SQL Server};'
                'Server=Catcrmdw1.cabsrv.org.uk;'
                f'Database={database};'
                'UID=Tableau_ReadOnly;'
                'PWD=B6h5vkrEhW5Mchg;')
    return conn, conn.cursor()

# It'd be cleaner to use context management and a with statement.
def _close_kcom_conn_and_cursor(conn, cursor):
    cursor.close()
    conn.close()