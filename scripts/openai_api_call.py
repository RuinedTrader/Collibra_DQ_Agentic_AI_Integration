from openai import OpenAI
import dotenv
import os



def generate_query(business_rules):

    dotenv.load_dotenv('../keys/.env')
    API_KEY = os.getenv('OPENAI_API_KEY')

    client = OpenAI(api_key=API_KEY)

    response = client.responses.create(
        model="gpt-4.1",
        instructions="""You are a GCP bigquery generator.
                        Users will send you business language rules that needs to be converted into a query.
                        The query should contain generic placeholders in the format -  <schema>, <table> and <column>
                        The query should provide two aggregate values - 'total_rows' and 'valid_rows' that provide total rows present in the table and the number of rows that pass the business rule criteria
                        Example - user message- 'The field should not be null or blank' 
                        Output Text Format - SELECT COUNT(*) AS total_rows, COUNTIF(<column> IS NOT NULL AND TRIM(<column>) != '') AS valid_rows FROM <schema>.<table>
                        The response should not contain any information other than the query in the specified format.""",
        input= business_rules
    )

    return response.output_text.replace('\n','')
