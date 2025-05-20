import json
from langchain_core.prompts import ChatPromptTemplate
from my_agent.LLMManager import LLMManager
from my_agent.DatabaseManager import DatabaseManager
from my_agent.graph_instructions import graph_instructions
import os
import pandas as pd

class DataVisualizer:
    def __init__(self):
        self.llm_manager = LLMManager()
        self.db_manager = DatabaseManager()    

    def code_generator_for_visualization(self, state: dict) -> str:
        """Generate code for the specified visualization."""
        
        parsed_question = state['parsed_question']
        datacolumns = parsed_question["relevant_tables"][0]['columns']
        visualization = state['visualization']
        reason = state['visualization_reason']
        
        sql_results = state['results']
        data = pd.DataFrame(sql_results).head()
        print(f"\n====> Data: {data}")
        sql_query = state["sql_query"]

        host= state['host']
        user=state['user']
        password=state['password']
        database=state['database']
        
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
        
        visual_code = self.llm_manager.invoke(prompt, sql_query=sql_query, visualization=visualization, userprompt= reason, data=data, datacolumns=datacolumns, host=host, user=user, password=password, database=database)
        print("\n====Visualization Code Generated====")

        with open('VS_code.py', 'w') as f:
            codes = visual_code.split('\n')
            code = "\n".join(codes[1:-1])
            print("\n====Cleaned Code====")
            f.write(code)

        return {"visualization_code" : code}
    
    def generate_visualization(self, state: str) -> str:
        """Format the generated code for better readability."""

        # Here you can add any formatting logic you need
        code = self.code_generator_for_visualization(state)
        
        print(f"\n>>>>> Generated code:\n{code}")

        print(f"\nGenerating Chart..........")

        os.system(f"python VS_code.py")
        print(f"\n********** Chart Generated **********")
        return {"visualization_status" : "success"}
    


# Generating Chart..........
# Data: Index(['Unnamed: 0', 'car name', 'brand', 'model', 'vehicle age', 'km driven',
#        'seller type', 'fuel type', 'transmission type', 'mileage', 'engine',
#        'max power', 'seats', 'selling price'],
#       dtype='object')
# Traceback (most recent call last):
#   File "E:\ViitorCloud\Projects\LangGraph_Data_Visualization\backend\VS_code.py", line 15, in <module>
#     plt.legend(title="Fuel Types", labels=fuel_type_counts.index, bbox_to_anchor=(1.05, 1), loc='upper left', bbox_transform=plt.gcf.transFigure)
#                                                                                                                              ^^^^^^^^^^^^^^^^^^^
# AttributeError: 'function' object has no attribute 'transFigure'