import requests
import os
import json
from typing import List, Dict, Any, TypedDict, Optional
from typing_extensions import Annotated
import operator
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END

from my_agent.WorkflowManager import WorkflowManager
from my_agent.DatabaseManager import DatabaseManager
from my_agent.LLMManager import LLMManager
import uuid

import pandas as pd
from decimal import Decimal





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

                debug_print(f"\n\nTable: {ind+1}\n{table}: {list(columns)}")
            
            else:
                debug_print(f"Table '{table}' is empty.")

    # Get table structure
        data_structure = {
            "schema": schema,
            "all_columns_names": all_columns,
            "all_table_names": all_tables
        }
        debug_print(f"\n\n{data_structure}")

        return data_structure
    
    else:
        return "No tables found in database"
    
try:    
    host="127.0.0.1"
    user="root"
    password="Mysqlharsh17@"
    database="python" #"blinkit"
    
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
            # debug_print(f"\n\nData Structure:\n{data_structures}\n\n")
            
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






## SQLAgent.py


######### def init
# db_manager = DatabaseManager()
llm_manager = LLMManager()

######### def parse_question
"""Parse user question and identify relevant tables and columns."""

question = """Calculate the average delivery time for all orders in each city.
Assumption: Considering the customers whose order status is completed"""


question = """Find the top 3 customers based on the total order value they have placed.
Assumption: Considering the customers whose order status is completed"""


question = """Retrieve the top 3 most frequently ordered products in Mumbai.
Assumption: Considering the customers whose order status is completed"""

question = """Identify the number of customers who have not placed an order in the last 30 days."""

question = """Calculate the total revenue generated by each store.
Assumption: Considering the revenue from those customers whose order status is
completed"""

# ***
# question = """Write a SQL query to identify customers who placed only one order in the last 3 months.
# Assumption: Assuming 3 months as 90 days"""

question = """give a chart of cities with high percentages of customers who ordered only one time."""

# ***
# question = """Analyze the relationship between the distance covered by delivery agents and the
# average delivery time. give chart"""

schema = data_structures['schema']

prompt = ChatPromptTemplate.from_messages([
            ("system", '''You are a data analyst that can help summarize SQL tables and parse user questions about a database. 
                Given the question and database schema, identify the relevant tables and columns. 
                If the question is not relevant to the database or if there is not enough information to answer the question, set is_relevant to false.

                Your response should be in the following JSON format:
                {{
                    "is_relevant": boolean,
                    "relevant_tables": [
                        {{
                            "table_names": [string],
                            "columns": [string],
                            "noun_columns": [string]
                        }}
                    ]
                }}

                The "noun_columns" field should contain only the columns that are relevant to the question and contain nouns or names, for example, the column "Artist name" contains nouns relevant to the question "What are the top selling artists?", but the column "Artist ID" is not relevant because it does not contain a noun. Do not include columns that contain numbers.
            '''),
            ("human", "===Database schema:\n{schema}\n\n===User question:\n{question}\n\nIdentify relevant tables and columns:")
        ])

output_parser = JsonOutputParser()

response = llm_manager.invoke(prompt, schema=schema, question=question)
parsed_response = output_parser.parse(response)
debug_print({"parsed_question": parsed_response})










# def get_unique_nouns(self, state: dict) -> dict:


# """Find unique nouns in relevant tables and columns."""

# parsed_question = parsed_response  #state['parsed_question']

# if not parsed_question['is_relevant']:
#     debug_print({"unique_nouns": []})

# unique_nouns = set()
# for table_info in parsed_question['relevant_tables']:
#     table_name = table_info['table_names']
#     noun_columns = table_info['columns']
    
#     if noun_columns:
#         column_names = ', '.join(f"`{col}`" for col in noun_columns)
#         query = f"SELECT DISTINCT {column_names} FROM `{table_name[0]}`"
#         # results = self.db_manager.execute_query(state['uuid'], query)

#         host="127.0.0.1"
#         user="root"
#         password="Mysqlharsh17@"
#         database="blinkit"
        
#         conn = connect_to_database(host, user, password, database)
#         cursor = conn.cursor(dictionary=True)
#         cursor.execute(query)
#         results = cursor.fetchall()
#         conn.close()
#         debug_print("Connection closed.")

#         for row in results:
#             unique_nouns.update(str(value) for value in row if value)

# debug_print({"unique_nouns": list(unique_nouns)})










# def generate_sql(self, state: dict) -> dict:

"""Generate SQL query based on parsed question and unique nouns."""

question = question
parsed_question = parsed_response
unique_nouns = parsed_question["relevant_tables"][0]['columns']

if not parsed_question['is_relevant']:
    debug_print({"sql_query": "NOT_RELEVANT", "is_relevant": False})

schema = data_structures['schema']

prompt = ChatPromptTemplate.from_messages([
    ("system", '''
        You are an AI assistant that generates SQL queries based on user questions, database schema, and unique nouns found in the relevant tables. Generate a valid SQL query to answer the user's question.

        If there is not enough information to write a SQL query, respond with "NOT_ENOUGH_INFO".

        Here are some examples:

        1. What is the top selling product?
        Answer: SELECT product_name, SUM(quantity) as total_quantity FROM sales GROUP BY product_name ORDER BY total_quantity DESC LIMIT 1

        2. What is the total revenue for each product?
        Answer: SELECT product name, SUM(quantity * price) as total_revenue FROM sales GROUP BY product name  ORDER BY total_revenue DESC

        3. What is the market share of each product?
        Answer: SELECT product name, SUM(quantity) * 100.0 / (SELECT SUM(quantity) FROM sa  les) as market_share FROM sales GROUP BY product name  ORDER BY market_share DESC LIMIT 5

        4. Plot the distribution of income over time
        Answer: SELECT income, COUNT(*) as count FROM users GROUP BY income

        THE RESULTS SHOULD ONLY BE IN THE FOLLOWING FORMAT, SO MAKE SURE TO ONLY GIVE TWO OR THREE COLUMNS:
        [[x, y]]
        or 
        [[label, x, y]]
                    
        give SQL query in simple text without adding "`".
            
        For questions like "plot a distribution of the fares for men and women", count the frequency of each fare and plot it. The x axis should be the fare and the y axis should be the count of people who paid that fare.
        SKIP ALL ROWS WHERE ANY COLUMN IS NULL or "N/A" or "".
        Just give the query string. Do not format it. Make sure to use the correct spellings of nouns as provided in the unique nouns list. All the table and column names should be enclosed in backticks.
    '''),
            ("human", '''===Database schema:
                {schema}

                ===User question:
                {question}

                ===Relevant tables and columns:
                {parsed_question}

                ===Unique nouns in relevant tables:
                {unique_nouns}

                Generate SQL query string
             '''),
        ])

sql_response = llm_manager.invoke(prompt, schema=schema, question=question, parsed_question=parsed_question, unique_nouns=unique_nouns)

if response.strip() == "NOT_ENOUGH_INFO":
    debug_print({"sql_query": "NOT_RELEVANT"})
else:
    debug_print({"sql_query": response})








# def validate_and_fix_sql(self, state: dict) -> dict:


"""Validate and fix the generated SQL query."""

sql_query = sql_response

if sql_query == "NOT_RELEVANT":
    debug_print({"sql_query": "NOT_RELEVANT", "sql_valid": False})

schema = data_structures['schema']


prompt = ChatPromptTemplate.from_messages([
    ("system", '''
        You are an AI assistant that validates and fixes SQL queries. Your task is to:
        1. Check if the SQL query is valid.
        2. Ensure all table and column names are correctly spelled and exist in the schema. All the table and column names should be enclosed in backticks.
        3. If there are any issues, fix them and provide the corrected SQL query.
        4. If no issues are found, return the original query.

        Respond in JSON format with the following structure. Only respond with the JSON:
        {{
            "valid": boolean,
            "issues": string or null,
            "corrected_query": string
        }}
        '''),
                    ("human", '''===Database schema:
        {schema}

        ===Generated SQL query:
        {sql_query}

        Respond in JSON format with the following structure. Only respond with the JSON:
        {{
            "valid": boolean,
            "issues": string or null,
            "corrected_query": string
        }}

        For example:
        1. {{
            "valid": true,
            "issues": null,
            "corrected_query": "None"
        }}
                    
        2. {{
            "valid": false,
            "issues": "Column USERS does not exist",
            "corrected_query": "SELECT * FROM users WHERE age > 25 LIMIT 3"
        }}

        3. {{
            "valid": false,
            "issues": "Column names and table names should be enclosed in backticks if they contain spaces or special characters",
            "corrected_query": "SELECT * FROM gross income WHERE age > 25 LIMIT 5"
        }}
                    
    '''),
])

output_parser = JsonOutputParser()
response = llm_manager.invoke(prompt, schema=schema, sql_query=sql_query)
sql_query = output_parser.parse(response)

if sql_query["valid"] and sql_query["issues"] is None:
    debug_print({"sql_query": sql_query, "sql_valid": True})
else:
    debug_print({
        "sql_query": sql_query["corrected_query"],
        "sql_valid": sql_query["valid"],
        "sql_issues": sql_query["issues"]
    })








# def execute_sql(self, state: dict) -> dict:

"""Execute the SQL query and return the results."""

query = sql_query["corrected_query"]
        
if query == "NOT_RELEVANT":
    debug_print({"results": "NOT_RELEVANT"})

# try:
    # results = self.db_manager.execute_query(uuid, query)

conn = connect_to_database(host, user, password, database)
cursor = conn.cursor(dictionary=True)
cursor.execute(query)
sql_results = cursor.fetchall()
conn.close()
debug_print("Connection closed.")

debug_print({"results": sql_results})
# except Exception as e:
#     debug_print({"error": str(e)})











# def format_results(self, state: dict) -> dict:


"""Format query results into a human-readable response."""

question = question
results = sql_results

if results == "NOT_RELEVANT":
    print({"answer": "Sorry, I can only give answers relevant to the database."})

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are an AI assistant that formats database query results into a human-readable response. Give a conclusion to the user's question based on the query results. Do not give the answer in markdown format."),
    ("human", "User question: {question}\n\nQuery results: {results}\n\nFormatted response:"),
])

response = llm_manager.invoke(prompt, question=question, results=results)
print({"answer": response})









# def choose_visualization(self, state: dict) -> dict:


"""Choose an appropriate visualization for the data."""

sql_query = sql_query["corrected_query"]
sql_results = sql_results

if sql_results == "NOT_RELEVANT":
    debug_print({"visualization": "none", "visualization_reasoning": "No visualization needed for irrelevant questions."})

prompt = ChatPromptTemplate.from_messages([
    ("system", '''
        You are an AI assistant that recommends appropriate data visualizations. Based on the user's question, SQL query, and query results, suggest the most suitable type of graph or chart to visualize the data. If no visualization is appropriate, indicate that.

        Available chart types and their use cases:
        - Bar Graphs: Best for comparing categorical data or showing changes over time when categories are discrete and the number of categories is more than 2. Use for questions like "What are the sales figures for each product?" or "How does the population of cities compare? or "What percentage of each city is male?"
        - Horizontal Bar Graphs: Best for comparing categorical data or showing changes over time when the number of categories is small or the disparity between categories is large. Use for questions like "Show the revenue of A and B?" or "How does the population of 2 cities compare?" or "How many men and women got promoted?" or "What percentage of men and what percentage of women got promoted?" when the disparity between categories is large.
        - Scatter Plots: Useful for identifying relationships or correlations between two numerical variables or plotting distributions of data. Best used when both x axis and y axis are continuous. Use for questions like "Plot a distribution of the fares (where the x axis is the fare and the y axis is the count of people who paid that fare)" or "Is there a relationship between advertising spend and sales?" or "How do height and weight correlate in the dataset? Do not use it for questions that do not have a continuous x axis."
        - Pie Charts: Ideal for showing proportions or percentages within a whole. Use for questions like "What is the market share distribution among different companies?" or "What percentage of the total revenue comes from each product?"
        - Line Graphs: Best for showing trends and distributionsover time. Best used when both x axis and y axis are continuous. Used for questions like "How have website visits changed over the year?" or "What is the trend in temperature over the past decade?". Do not use it for questions that do not have a continuous x axis or a time based x axis.

        Consider these types of questions when recommending a visualization:
        1. Aggregations and Summarizations (e.g., "What is the average revenue by month?" - Line Graph)
        2. Comparisons (e.g., "Compare the sales figures of Product A and Product B over the last year." - Line or Column Graph)
        3. Plotting Distributions (e.g., "Plot a distribution of the age of users" - Scatter Plot)
        4. Trends Over Time (e.g., "What is the trend in the number of active users over the past year?" - Line Graph)
        5. Proportions (e.g., "What is the market share of the products?" - Pie Chart)
        6. Correlations (e.g., "Is there a correlation between marketing spend and revenue?" - Scatter Plot)

        Provide your response in the following format:
        Recommended Visualization: [Chart type or "None"]. ONLY use the following names: bar, horizontal_bar, line, pie, scatter, none
        Reason: [Brief explanation for your recommendation and explain the conditions for data preprocessing to make the suitable chart in matplotlib. For example, 'the needs to be grouped by with specific columns, remove null values, conditions like greater, less than, equal to, etc. and sorted by a specific column.']

        Your response should be in the following JSON format:
        {{
            "visualization": string,
            "reason": string
        }}
    '''),

    ("human", '''
        User question: {question}
        SQL query: {sql_query}
        Query results: {results}

        Recommend a visualization:
        '''),
])

output_parser = JsonOutputParser()

visual_response = llm_manager.invoke(prompt, question=question, results=results, sql_query=sql_query)
parsed_response = output_parser.parse(visual_response)

visualization = parsed_response['visualization']
reason = parsed_response['reason']
print(f"\n>>>>> Visualization: {visualization}")
print(f"\n>>>>> Reason: {reason}")

# return {"visualization": visualization, "visualization_reason": reason}
debug_print({"visualization": visualization, "visualization_reason": reason, "sql_query": sql_query})







# def code_generator_for_visualization(self, datacolumns: list, visualization: str, reason: str, data: dict, path: str) -> str:
"""Generate code for the specified visualization."""

data = sql_results

data = pd.DataFrame(sql_results).head()
print(f"\n====> Data: {data}")
sql_query = query
datacolumns = data.columns.tolist()

prompt = ChatPromptTemplate.from_messages([
    ("system", '''
        You are an intelligent code generator for Data analysis that can generate code for data visualization. Based on the user's prompt, data and  type of chart specified, generate the code for the chart that is defined by user.
        you will use matplotlib library for data visualization.
        Strictly generate code for the chart only.
        Data should be read from the variable "data" which is a pandas dataframe and make preprocessing of data however needed and specified.
        Build logic based on SQL query and the data provided.
        the code should include x and y axis labels, title and legend.
        code should be in python and should be executable with indentation.
        here data is in Pandas dataframe format.
        give only the code without any explanation or comments.
        Do not generate any Data.
        use column names according to the column names provided{datacolumns}.
        Do not use this parameter in the code = "plt.gcf.transFigure".
        Strictly chart should be saved as a png file with the dynamic name "{visualization} + dynamic name.png".
        code should not contain any errors while running. it should be executable.
        check the sql query and make it correct if needed.
        code should be in simple text.
        code should include below lines:
        import pandas as pd
        import matplotlib.pyplot as plt
        import mysql.connector
        conn = mysql.connector.connect(
            host="{host}",
            user="{user}",
            password="{password}",
            database="{database}",
            connection_timeout=5,
            connect_timeout=5,
            use_pure=True
        )
        print(f"Connected to database '{database}'")
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""{sql_query}""")
        sql_results = cursor.fetchall()
        conn.close()
        print("Connection closed.")
        data = pd.DataFrame(sql_results)
        .
        .
        At lastline print the code with the message "=======> Chart Generated".
     '''),
    ("human", "===Data Sample:\n{data}===Type Of Chart:\n{visualization}\n\n===SQL Query:\n{sql_query}\n\n===prompt:\n{userprompt}\n\nGenerate code for the visualization:")
])

visual_code = llm_manager.invoke(prompt, sql_query=sql_query, visualization=visualization, userprompt= reason, data=data, datacolumns=datacolumns, host=host, user=user, password=password, database=database)
print("\n====Visualization Code Generated====")
debug_print({"visual_code": visual_code})


with open('VS_code.py', 'w') as f:
    codes = visual_code.split('\n')
    code = "\n".join(codes[1:-1])
    print("\n====Cleaned Code====")
    f.write(code)

print({"visualization_code" : code})
    
# def generate_visualization(self, state: str) -> str:
"""Format the generated code for better readability."""


print(f"\n>>>>> Generated code:\n{code}")

print(f"\nGenerating Chart..........")

os.system(f"python VS_code.py")
print(f"\n********** Chart Generated **********")
print({"visualization_status" : "success"})






# def format_code_response(self, datacolumns: list, visualization: str, reason: str, data: dict, path: str) -> str:


# """Format the generated code for better readability."""



# Here you can add any formatting logic you need
# response = self.code_generator_for_visualization(datacolumns, visualization, reason, data, path)

with open('VS_code.py', 'w') as f:
    codes = visual_code.split('\n')
    code = "\n".join(codes[1:-1])
    print("\n====Cleaned Code====")
    f.write(code)

debug_print(f"\n>>>>> Generated code:\n{code}")
# return code

# def generate_visualization(self, state: dict) -> str:
# """Generate the visualization code."""
# datacolumns = state['unique_nouns']
# visualization = state['visualization']
# reason = state['visualization_reason']
# path = state['path']
# df = pd.read_json(state['results']).head(10)

# print(f"\n====> Data: {df}")
# print("\n===Generating Code for Visualization===")
# data = df.to_json(orient="records")
# code = self.format_code_response(datacolumns, visualization, reason, data, path)

print(f"\nGenerating Chart..........")
os.system(f"python VS_code.py")
print(f"\n********** Chart Generated **********")
# return {"visualization_code" : code}

