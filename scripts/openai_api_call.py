from openai import OpenAI
import dotenv
import os
import scripts.structured_output_models as models

dotenv.load_dotenv('../keys/.env')
API_KEY = os.getenv('OPENAI_API_KEY')
client = OpenAI(api_key=API_KEY)

def generate_query(business_rules):

    response = client.responses.parse(
        model="gpt-4.1",
        instructions="""You are a GCP bigquery generator that converts business language rules that needs to be converted into a query.
                        The query should contain generic placeholders in the format -  <schema>, <table> and <column>
                        The query should provide two aggregate values - 'total_rows' and 'valid_rows' that provide total rows present in the table and the number of rows that pass the business rule criteria
                        Example - user message- 'The field should not be null or blank' 
                        Output Text Format - SELECT COUNT(*) AS total_rows, COUNTIF(<column> IS NOT NULL AND TRIM(<column>) != '') AS valid_rows FROM <schema>.<table>""",
        input= business_rules,
        text_format = models.QueryModel
    )

    return response.output_parsed.query





def generate_fix(schema, table, column, query, business_rule, dq_score, rows_passed, rows_failed, threshold, result,history):
    response = client.responses.parse(
        model="gpt-4.1",
        instructions="""
        You are a Data Quality Analyst Agent that receives the column details where DQ score dropped below threshold.

        You will be provided with:
        1. Schema and Table names
        2. The Column name
        2. English language rule on the column for query generation
        3. The BigQuery query that produced the DQ score.
        3. The DQ score (DQ score indicates the percentage of total rows that passed the implemented rule)
        4. Rows passing the rule
        5. Rows failing the rule
        5. The threshold value for the rule
        4. The DQ score history over time. 

        Your tasks:
        1. Analyze the provided BigQuery query to infer possible data issues.
        2. Analyse DQ score history to detect trends or time-based patterns. If the history has DQ score just for a single timeframe, mention accordingly that there is not much details to establish trend.
        2. Provide an elaborated english description of the issue.
        3. Return 3 safe fix strategies with corresponding SQL query to apply those fixes (Use only the provided details to generate the query).
        4. Return a confidence score ranging from 0 to 100 against each fix indicating safety and effectiveness.
        """,

        input=f"""
        DQ Score Report:
        Schema: {schema}
        Table: {table}
        Column: {column}
        Query used for score : {query}
        English language rule : {business_rule}
        DQ score: {dq_score}
        Rows passed: {rows_passed}
        Rows failed: {rows_failed}
        Threshold: {threshold}
        Pass : {result}
        History of DQ scores : {history}
        """,
        text_format=models.QuerySuggestionModel
    )
    return response.output_parsed






