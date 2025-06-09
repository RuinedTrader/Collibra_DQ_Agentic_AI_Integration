import yaml
from pydantic import BaseModel, Field
import yaml
import requests
from requests.auth import HTTPBasicAuth
from openai import OpenAI


with open('collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file, Loader=yaml.FullLoader)

collibra_host_endpoint = collibra_config['base_api_endpoint']


def find_asset(domain_id, asset_type_id):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'limit': 1000000, 'domainId': domain_id, 'typeId': asset_type_id}
    return requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')


def find_relations_by_source(source_asset_id, relation_type_id):
    url = collibra_host_endpoint + collibra_config["relation_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'relationTypeId': relation_type_id, 'sourceId': source_asset_id}
    return requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')


def find_relations_by_target(target_asset_id, relation_type_id):
    url = collibra_host_endpoint + collibra_config["relation_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'relationTypeId': relation_type_id, 'targetId': target_asset_id}
    return requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')


def find_asset_attribute_value(asset_id, attribute_type_id):
    url = collibra_host_endpoint + collibra_config["attribute_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'typeIds': attribute_type_id, 'assetId': asset_id}
    return requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')[0].get('value')


def add_attribute(asset_id, attribute_type_id, value):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"] + f'/{asset_id}/attributes'
    headers = {'Content-type': 'application/json', 'accept': 'application/json'}
    request_body = {"typeId": attribute_type_id, "values": [value]}
    requests.post(url, headers=headers, json=request_body, auth=auth)


def add_asset(asset_name, display_name, asset_type, domain_id, status_id):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"]
    headers = {'Content-type': 'application/json', 'accept': 'application/json'}
    request_body = {
        "name": asset_name,
        "displayName": display_name,
        "domainId": domain_id,
        "typeId": asset_type,
        "statusId": status_id
    }
    response = requests.post(url, json=request_body, headers=headers, auth=auth).json()
    return response["id"]


def add_relation(source_id, target_id, relation_type_id):
    url = collibra_host_endpoint + collibra_config["relation_endpoint"]
    headers = {'Content-type': 'application/json', 'accept': 'application/json'}
    request_body = {
        "sourceId": source_id,
        "targetId": target_id,
        "typeId": relation_type_id
    }
    requests.post(url, json=request_body, headers=headers, auth=auth)



class FixStrategyModel(BaseModel):
    fix_strategy: str = Field(description='English language description of the solution')
    fix_query: str = Field(description='GCP Bigquery to fix the issue')
    confidence_score: int


class QuerySuggestionModel(BaseModel):
    dq_issue_description: str = Field(description='Simple english language description of the issue')
    fix_strategies: list[FixStrategyModel] = Field(description='Three fixes for the detected DQ issue')

def generate_fix(schema,table,column,tech_query,business_rule,score,result):
    response = client.responses.parse(
        model="gpt-4.1",
        instructions="""
        You are an Data Quality Analyst Agent that receives those column details that have DQ score below the threshold value, along with the GCP bigquery and english language rule that was used to get the score
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
        DQ score: {score}
        Pass : {result}
        Query used to get the result : {tech_query}
        English language rule for the column : {business_rule} 
        """,
        text_format=QuerySuggestionModel
    )
    return response.output_text


# step 1: get the failed DQ metrics assets

dq_metric_response = find_asset(collibra_config['dqm_domain_id'],
                         collibra_config['data_quality_metric_asset_type_id'])


def generate_fix_strategies(request):
    dqm_id = request.get('id')

    passing_flag = find_asset_attribute_value(dqm_id, collibra_config['result_attribute_type_id'])  # Fetch result value

    if not passing_flag:  # step 2: if the threshold value of the DQ metric is not met, fetch table/column
        dq_score = find_asset_attribute_value(dqm_id, collibra_config['passing_fraction_attribute_type_id'])

        # get rule from dqm
        rule_dqm_rel = find_relations_by_target(dqm_id, collibra_config['rule_dqm_relation_type_id'])[0]
        rule_id = rule_dqm_rel.get('source').get('id')
        query = find_asset_attribute_value(rule_id, collibra_config['technical_rule_attribute_type_id'])
        rule_statement = find_asset_attribute_value(rule_id, collibra_config['rule_statement_attribute_type_id'])

        # get DE from rule
        de_rule_rel = find_relations_by_target(rule_id, collibra_config['data_element_rule_specification_relation_type_id'])[0]
        de_id = de_rule_rel.get('source').get('id')

        # get column/table
        column_de_relation = find_relations_by_target(de_id, collibra_config['column_data_element_relation_type_id'])[0]
        source_details = column_de_relation.get('source').get('name').split('.')
        schema_name = source_details[0]
        table_name = source_details[1]
        column_name = source_details[2]

        # step 3: run the agentic AI workflow script -> get suggestions on how to fix it -> provide the column/table/schema and the query that generated that result
        print('Generating fix...')
        fixes_response = generate_fix(schema_name,table_name,column_name,query,rule_statement,dq_score,passing_flag)

# step 4: notify the stakeholders regarding fix and provide an option to proceed or cancel the solution-> collibra wf
# step 5: if yes, connect to gcp big query and run the DML query


