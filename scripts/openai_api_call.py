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





def generate_fix(schema,table,column,query,business_rule,dq_score,result):
    response = client.responses.parse(
        model="gpt-4.1",
        instructions="""
        You are an Data Quality Analyst Agent that receives the column that have DQ score below the threshold value, along with the GCP bigquery and english language rule that was used to get the score
        Your job is to:
        1. Analyze the GCP Bigquery that produced the score to infer the type of data issue.
        2. Provide elaborated english language description of the issue.
        3. Return 3 safe fix strategies alongside the SQL query to apply those fixes.
        4. Return a confidence score ranging from 0 to 100 against each fix stating your fix is safe and effective.
        """,
        input=f"""
        DQ Score Report:
        Schema: {schema}
        Table: {table}
        Column: {column}
        DQ score: {dq_score}
        Pass : {result}
        Query used to get the result : {query}
        English language rule for the column : {business_rule} 
        """,
        text_format=models.QuerySuggestionModel
    )
    return response.output_parsed






