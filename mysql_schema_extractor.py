########## MySQL Connection Test Script ##########

import mysql.connector
import sys
import time

def debug_print(message):
    """Print a message with timestamp and flush to ensure it appears immediately."""
    timestamp = time.strftime("%H:%M:%S", time.localtime())
    print(f"[{timestamp}] {message}", flush=True)

debug_print("Starting MySQL connection test")
debug_print(f"Python version: {sys.version}")
debug_print(f"MySQL Connector version: {mysql.connector.__version__}")


def connect_to_database(host, user, password, database=None, connection_timeout=5, connect_timeout=5, use_pure=True):
    """Attempt to connect to the MySQL database."""
    try:
        if database:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                connection_timeout=5,
                connect_timeout=5,
                use_pure=True
            )
            print(f"Connected to database '{database}'")
        else:
            conn = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                connection_timeout=5,
                connect_timeout=5,
                use_pure=True
            )
            print(f"Connected to MySQL server but not to a {database} database")
        return conn
    except mysql.connector.Error as err:
        debug_print(f"Connection error: {err}")
        return None
    

def data_structure(connection):
    # List tables
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SHOW TABLES")
    tables = [list(table.values())[0] for table in cursor.fetchall()]

    
    schema = {}
    all_columns = []
    all_tables = []

    if tables:
        
        for ind, table in enumerate(tables):

            # Get sample data from first table
            # first_table = tables[ind]
            all_tables.append(table)

            cursor.execute(f"SELECT * FROM `{table}` Limit 1")
            rows = cursor.fetchall()
            

            if rows:
                columns = rows[0].keys()
                schema[table] = list(columns)
                all_columns.extend(columns)
            
            else:
                debug_print(f"Table '{table}' is empty.")

    # Get table structure
        data_structure = {
            "schema": schema,
            "all_columns_names": all_columns,
            "all_table_names": all_tables
        }
        print("\n\n",data_structure,"\n\n")

        return data_structure
    
    else:
        return "No tables found in database"
    
try:    
    host="127.0.0.1"
    user="root"
    password="Mysqlharsh17@"
    database="blinkit"
    
    conn = connect_to_database(host, user, password, database)

    if conn:
        # Check for databases
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW DATABASES")
        databases = [db['Database'] for db in cursor.fetchall()]
        
        debug_print(f"Available databases: {', '.join(databases)}")
    
        if database in databases:
            
            debug_print("Target database 'blinkit' exists!")
            data_structures = data_structure(conn)
            debug_print(f"\n\nData Structure:\n{data_structures}\n\n")
            
        else:
            debug_print("ERROR: Database 'blinkit' does not exist!")
    else:
        debug_print("Connection to MySQL server failed.")

    conn.close()
    debug_print("Connection closed.")
    
except mysql.connector.Error as err:
    debug_print(f"MySQL Error: {err}")
    debug_print(f"Error code: {getattr(err, 'errno', 'N/A')}")
    debug_print(f"Error message: {getattr(err, 'msg', 'N/A')}")
    
    # Check for common issues
    if getattr(err, 'errno', None) == 1045:  # Access denied
        debug_print("Authentication failed - check username and password")
    elif getattr(err, 'errno', None) == 2003:  # Can't connect
        debug_print("Can't connect to MySQL server - check if server is running")
    elif getattr(err, 'errno', None) == 1049:  # Unknown database
        debug_print("Database doesn't exist")
        
except Exception as e:
    debug_print(f"Unexpected error: {e}")
    debug_print(f"Error type: {type(e).__name__}")
    
debug_print("Test complete")









