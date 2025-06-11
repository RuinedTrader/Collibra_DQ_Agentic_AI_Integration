from pydantic import BaseModel, Field


class FixStrategyModel(BaseModel):
    fix_strategy: str = Field(description='English description of the solution')
    fix_query: str = Field(description='Bigquery SQL to apply the fix')
    confidence_score: int = Field(description='Confidence score 0-100')


class QuerySuggestionModel(BaseModel):
    dq_issue_description: str = Field(description='English language description of the issue')
    trend_analysis : str = Field(description='Trend pattern in DQ scores')
    fix_strategies: list[FixStrategyModel] = Field(description='Three fixes for the detected DQ issue')


class QueryModel(BaseModel):
    query: str = Field(description='The query generated against the english language rule')
