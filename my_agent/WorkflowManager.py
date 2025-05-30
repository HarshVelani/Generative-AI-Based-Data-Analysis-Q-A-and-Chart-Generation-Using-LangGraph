from langgraph.graph import StateGraph
from my_agent.State import InputState, OutputState
from my_agent.SQLAgent import SQLAgent
from my_agent.DataVisualizer import DataVisualizer
from langgraph.graph import END

class WorkflowManager:
    def __init__(self):
        self.sql_agent = SQLAgent()
        self.data_visualizer = DataVisualizer()

    def create_workflow(self) -> StateGraph:
        """Create and configure the workflow graph."""
        workflow = StateGraph(input=InputState, output=OutputState)

        # Add nodes to the graph
        workflow.add_node("data_structured", self.sql_agent.data_structured)
        workflow.add_node("parse_question", self.sql_agent.parse_question)
        workflow.add_node("generate_sql", self.sql_agent.generate_sql)
        workflow.add_node("validate_and_fix_sql", self.sql_agent.validate_and_fix_sql)
        workflow.add_node("execute_sql", self.sql_agent.execute_sql)
        workflow.add_node("format_results", self.sql_agent.format_results)
        workflow.add_node("choose_visualization", self.sql_agent.choose_visualization)
        workflow.add_node("code_generator_for_visualization", self.data_visualizer.code_generator_for_visualization)
        workflow.add_node("generate_visualization", self.data_visualizer.generate_visualization)
        
        # Define edges
        workflow.add_edge("data_structured", "parse_question")
        workflow.add_edge("parse_question", "generate_sql")
        workflow.add_edge("generate_sql", "validate_and_fix_sql")
        workflow.add_edge("validate_and_fix_sql", "execute_sql")
        workflow.add_edge("execute_sql", "format_results")
        workflow.add_edge("execute_sql", "choose_visualization")
        workflow.add_edge("choose_visualization", "code_generator_for_visualization")
        workflow.add_edge("code_generator_for_visualization", "generate_visualization")
        workflow.add_edge("generate_visualization", END)
        workflow.add_edge("format_results", END)
        workflow.set_entry_point("data_structured")

        return workflow
    
    def returnGraph(self):
        return self.create_workflow().compile()

    def run_sql_agent(self, question: str) -> dict:
        """Run the SQL agent workflow and return the formatted answer and visualization recommendation."""
        app = self.create_workflow().compile()
        result = app.invoke({"question": question})
        return {
            "answer": result['answer'],
            "visualization": result['visualization'],
            "visualization_reason": result['visualization_reason'],
            "formatted_data_for_visualization": result['formatted_data_for_visualization']
        }