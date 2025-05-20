from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from my_agent.DatabaseManager import DatabaseManager
from my_agent.LLMManager import LLMManager

class SQLAgent:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.llm_manager = LLMManager()


    def data_structured(self, state: dict) -> dict:

        host = state['host']
        user = state['user']
        password = state['password']
        database = state['database']

        connection = self.db_manager.connect_to_database(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
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

                    print(f"\n\nTable: {ind+1}\n{table}: {list(columns)}")
                
                else:
                    print(f"Table '{table}' is empty.")

        # Get table structure
            data_structure = {
                "schema": schema,
                "all_columns_names": all_columns,
                "all_table_names": all_tables
            }
            print(f"\n\n{data_structure}")

            connection.close()
            print("Connection closed.")

            return {"data_structure": data_structure}
        
        else:
            return "No tables found in database"
        



    def parse_question(self, state: dict) -> dict:
        """Parse user question and identify relevant tables and columns."""
        question = state['question']
        schema = state['data_structure']['schema']

        prompt = ChatPromptTemplate.from_messages([
            ("system", '''You are a data analyst that can help summarize SQL tables and parse user questions about a database. 
                Given the question and database schema, identify the relevant tables and columns. 
                If the question is not relevant to the database or if there is not enough information to answer the question, set is_relevant to false.

                Your response should be in the following JSON format:
                {{
                    "is_relevant": boolean,
                    "relevant_tables": [
                        {{
                            "table_name": string,
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
        
        response = self.llm_manager.invoke(prompt, schema=schema, question=question)
        parsed_response = output_parser.parse(response)
        return {"parsed_question": parsed_response}

    # def get_unique_nouns(self, state: dict) -> dict:
    #     """Find unique nouns in relevant tables and columns."""
    #     parsed_question = state['parsed_question']
        
    #     if not parsed_question['is_relevant']:
    #         return {"unique_nouns": []}

    #     unique_nouns = set()
    #     for table_info in parsed_question['relevant_tables']:
    #         table_name = table_info['table_name']
    #         noun_columns = table_info['columns']
            
    #         if noun_columns:
    #             column_names = ', '.join(f"`{col}`" for col in noun_columns)
    #             query = f"SELECT DISTINCT {column_names} FROM `{table_name}`"
    #             results = self.db_manager.execute_query(state['uuid'], query)
    #             for row in results:
    #                 unique_nouns.update(str(value) for value in row if value)

    #     return {"unique_nouns": list(unique_nouns)}

    def generate_sql(self, state: dict) -> dict:
        """Generate SQL query based on parsed question and unique nouns."""
        question = state['question']
        parsed_question = state['parsed_question']
        unique_nouns = parsed_question["relevant_tables"][0]['columns']

        if not parsed_question['is_relevant']:
            return {"sql_query": "NOT_RELEVANT", "is_relevant": False}
    
        schema = state['data_structure']['schema']

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

        response = self.llm_manager.invoke(prompt, schema=schema, question=question, parsed_question=parsed_question, unique_nouns=unique_nouns)
        
        print(f"\n\nSQL Query: {response}")
        if response.strip() == "NOT_ENOUGH_INFO":
            return {"sql_query": "NOT_RELEVANT"}
        else:
            return {"sql_query": response}
        



    def validate_and_fix_sql(self, state: dict) -> dict:
        """Validate and fix the generated SQL query."""
        sql_query = state['sql_query']

        if sql_query == "NOT_RELEVANT":
            return {"sql_query": "NOT_RELEVANT", "sql_valid": False}
        
        schema = state['data_structure']['schema']

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
        response = self.llm_manager.invoke(prompt, schema=schema, sql_query=sql_query)
        result = output_parser.parse(response)

        print(f"\n\nValidation Result: {result["corrected_query"]}")

        if result["valid"] and result["issues"] is None:
            return {"sql_query": sql_query, "sql_valid": True}
        else:
            return {
                "sql_query": result["corrected_query"],
                "sql_valid": result["valid"],
                "sql_issues": result["issues"]
            }

    def execute_sql(self, state: dict) -> dict:
        """Execute SQL query and return results."""
        query = state['sql_query']
        # uuid = state['uuid']
        host = state['host']
        user = state['user']
        password = state['password']
        database = state['database']

        connection = self.db_manager.connect_to_database(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # List tables
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        connection.close()
        print("Connection closed.")
        
        if query == "NOT_RELEVANT":
            return {"results": "NOT_RELEVANT"}

        try:
            return {"results": results}
        except Exception as e:
            return {"error": str(e)}


    def format_results(self, state: dict) -> dict:
        """Format query results into a human-readable response."""
        question = state['question']
        results = state['results']

        if results == "NOT_RELEVANT":
            return {"answer": "Sorry, I can only give answers relevant to the database."}

        prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an AI assistant that formats database query results into a human-readable response. Give a conclusion to the user's question based on the query results. Do not give the answer in markdown format."),
            ("human", "User question: {question}\n\nQuery results: {results}\n\nFormatted response:"),
        ])

        response = self.llm_manager.invoke(prompt, question=question, results=results)
        return {"answer": response}


    def choose_visualization(self, state: dict) -> dict:
        """Choose an appropriate visualization for the data."""
        question = state['question']
        results = state['results']
        sql_query = state['sql_query']

        if results == "NOT_RELEVANT":
            return {"visualization": "none", "visualization_reasoning": "No visualization needed for irrelevant questions."}

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

                Recommend a visualization:'''),
        ])

        output_parser = JsonOutputParser()

        visual_response = self.llm_manager.invoke(prompt, question=question, results=results, sql_query=sql_query)
        parsed_response = output_parser.parse(visual_response)

        visualization = parsed_response['visualization']
        reason = parsed_response['reason']
        print(f"\n>>>>> Visualization: {visualization}")
        print(f"\n>>>>> Reason: {reason}")

        # return {"visualization": visualization, "visualization_reason": reason}
        print({"visualization": visualization, "visualization_reason": reason, "sql_query": sql_query})
        return {"visualization": visualization, "visualization_reason": reason}