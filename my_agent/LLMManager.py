from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from dotenv import load_dotenv
load_dotenv()

class LLMManager:
    def __init__(self):
        self.llm = ChatGroq(model="meta-llama/llama-4-scout-17b-16e-instruct", temperature=0, api_key="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")

    def invoke(self, prompt: ChatPromptTemplate, **kwargs) -> str:
        messages = prompt.format_messages(**kwargs)
        response = self.llm.invoke(messages)
        return response.content