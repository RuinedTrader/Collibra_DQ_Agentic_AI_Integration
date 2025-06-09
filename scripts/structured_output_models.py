from pydantic import BaseModel, Field


class FixStrategyModel(BaseModel):
    fix_strategy: str = Field(description='English language description of the solution')
    fix_query: str = Field(description='GCP Bigquery to fix the issue')
    confidence_score: int


class QuerySuggestionModel(BaseModel):
    dq_issue_description: str = Field(description='Simple english language description of the issue')
    fix_strategies: list[FixStrategyModel] = Field(description='Three fixes for the detected DQ issue')


class QueryModel(BaseModel):
    query: str = Field(description='The query that is generated against the english language rule')
