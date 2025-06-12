import json
import random
import yaml
from datetime import datetime
import functions_framework
from requests.auth import HTTPBasicAuth
from openai import OpenAI
import requests
from pydantic import BaseModel, Field


with open('collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file, Loader=yaml.FullLoader)

collibra_host_endpoint = collibra_config['base_api_endpoint']
username = collibra_config['collibra_username']
password = collibra_config['collibra_password']
openai_api_key = collibra_config['openai_api_key']

auth = HTTPBasicAuth(username, password)
client = OpenAI(api_key=openai_api_key)


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
    attribute =  requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')
    if len(attribute) != 0:
        return attribute[0].get('value')
    return None


def add_attribute(asset_id, attribute_type_id, value):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"] + f'/{asset_id}/attributes'
    headers = {'Content-type': 'application/json', 'accept': 'application/json'}
    request_body = {"typeId": attribute_type_id, "values": [value]}
    requests.put(url, headers=headers, json=request_body, auth=auth)


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


def search_activities_for_attributes(context_id):
    url = collibra_host_endpoint + collibra_config["activities_endpoint"]
    headers = {'accept': '*/*'}
    params = {'contextId': context_id, 'categories': 'ATTRIBUTE', 'resourceDiscriminators': 'NumericAttribute',
              'activityType': 'ADD'}

    return requests.get(url, params=params, headers=headers, auth=auth).json().get('results')


def get_date_from_epoch(epoch_time):
    formatted_date = datetime.fromtimestamp(timestamp=epoch_time / 1000).strftime("%Y/%m/%d")
    return formatted_date


class FixStrategyModel(BaseModel):
    fix_strategy: str = Field(description='English description of the solution')
    fix_query: str = Field(description='Bigquery SQL to apply the fix')
    confidence_score: int = Field(description='Confidence score 0-100')


class QuerySuggestionModel(BaseModel):
    dq_issue_description: str = Field(description='English language description of the issue')
    trend_analysis: str = Field(description='Trend pattern in DQ scores')
    fix_strategies: list[FixStrategyModel] = Field(description='Three fixes for the detected DQ issue')


def generate_fix(schema, table, column, query, business_rule, dq_score, rows_passed, rows_failed, threshold, result,
                 history):
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
        text_format=QuerySuggestionModel
    )
    return response.output_parsed


@functions_framework.http
def generate_data_concern_and_fix(request):
    dq_metric_response = find_asset(collibra_config['dqm_domain_id'],
                                    collibra_config['data_quality_metric_asset_type_id'])

    for each_dqm in dq_metric_response:
        dqm_id = each_dqm.get('id')

        passing_flag = find_asset_attribute_value(dqm_id,
                                                  collibra_config['result_attribute_type_id'])  # Fetch result value

        if not passing_flag:
            dq_score = find_asset_attribute_value(dqm_id, collibra_config['passing_fraction_attribute_type_id'])
            rows_passed = find_asset_attribute_value(dqm_id, collibra_config['rows_passed_attribute_type_id'])
            rows_failed = find_asset_attribute_value(dqm_id, collibra_config['rows_failed_attribute_type_id'])
            threshold = find_asset_attribute_value(dqm_id, collibra_config['threshold_attribute_type_id'])

            # get rule from dqm
            rule_dqm_rel = find_relations_by_target(dqm_id, collibra_config['rule_dqm_relation_type_id'])[0]
            rule_id = rule_dqm_rel.get('source').get('id')
            query = find_asset_attribute_value(rule_id, collibra_config['technical_rule_attribute_type_id'])
            rule_statement = find_asset_attribute_value(rule_id, collibra_config['rule_statement_attribute_type_id'])

            # get DE from rule
            de_rule_rel = find_relations_by_target(rule_id, collibra_config['data_element_rule_specification_relation_type_id'])[0]
            de_id = de_rule_rel.get('source').get('id')
            de_name = de_rule_rel.get('source').get('name')

            # get column/table
            column_de_relation = find_relations_by_target(de_id, collibra_config['column_data_element_relation_type_id'])[0]
            source_details = column_de_relation.get('source').get('name').split('.')
            schema_name = source_details[0]
            table_name = source_details[1]
            column_name = source_details[2]

            dqm_activity = search_activities_for_attributes(dqm_id)

            dq_history = {}
            for each_activity in dqm_activity:
                description = json.loads(each_activity['description'])
                if description.get('field') != 'Threshold' and description.get('field') != 'Loaded Rows':
                    epoch_val = each_activity['timestamp']
                    activity_datetime = get_date_from_epoch(epoch_val)
                    field = description['field']
                    value = description['new']['name']
                    if dq_history.get(activity_datetime) is not None:
                        x = dq_history[activity_datetime]
                        x.append({field: value})
                    else:
                        dq_history[activity_datetime] = [{field: value}]

            fixes_response = generate_fix(schema_name, table_name, column_name, query, rule_statement, dq_score,
                                          rows_passed, rows_failed, threshold, passing_flag, dq_history)

            dq_issue = fixes_response.dq_issue_description
            trend_analysis = fixes_response.trend_analysis

            count = 1
            resolution = ""
            for each_fix in fixes_response.fix_strategies:
                resolution = resolution + f"<p><strong>Resolution {count} :</strong>{each_fix.fix_strategy}</p><p><strong>Query :</strong> <code>{each_fix.fix_query}</code></p><p><strong>Confidence :</strong>{each_fix.confidence_score}%</p><br>"
                count += 1

            di_dqm_relations = find_relations_by_target(dqm_id, collibra_config[
                'data_concern_dqm_relation_type_id'])

            if len(di_dqm_relations) != 0:
                di_id = di_dqm_relations[0].get('source').get('id')
            else:
                di_name = 'Data_Issue_' + de_name + '_' + str(random.randint(300000, 400000))
                di_id = add_asset(di_name, di_name, collibra_config['data_issue_asset_type_id'],collibra_config['data_issue_domain_id'],collibra_config['approved_status_id'])
                add_relation(de_id, di_id, collibra_config['data_element_data_issue_relation_type_id'])
                add_relation(di_id, dqm_id, collibra_config['data_concern_dqm_relation_type_id'])

            add_attribute(di_id, collibra_config['description_attribute_type_id'], dq_issue)
            add_attribute(di_id, collibra_config['trend_analysis_attribute_type_id'], trend_analysis)
            add_attribute(di_id, collibra_config['resolution_attribute_type_id'], resolution)


    return 'Run Complete'


