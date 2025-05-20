from typing import List, Any, Annotated, Dict, Optional
from typing_extensions import TypedDict
import operator

class InputState(TypedDict):
    host: str
    user: str
    password: str
    database: str
    connection_timeout: int
    connect_timeout: int
    use_pure: bool
    data_structure: Dict[str, Any]
    question: str
    uuid: str
    parsed_question: Dict[str, Any]
    unique_nouns: List[str]
    sql_query: str
    results: List[Any]
    visualization: Annotated[str, operator.add]
    visualization_reason: Annotated[str, operator.add]
    visualization_code: str

class OutputState(TypedDict):
    parsed_question: Dict[str, Any]
    unique_nouns: List[str]
    sql_query: str
    sql_valid: bool
    sql_issues: str
    sql_query: str
    results: List[Any]
    answer: Annotated[str, operator.add]
    error: str
    visualization: Annotated[str, operator.add]
    visualization_reason: Annotated[str, operator.add]
    visualization_code: str
    visualization_status: str