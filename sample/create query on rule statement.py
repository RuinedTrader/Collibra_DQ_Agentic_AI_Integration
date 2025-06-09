import yaml
import requests
from requests.auth import HTTPBasicAuth
from openai import OpenAI
from pydantic import BaseModel, Field


with open('collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file, Loader=yaml.FullLoader)

collibra_host_endpoint = collibra_config['base_api_endpoint']


def generate_query(business_rules):
    response = client.responses.parse(
        model="gpt-4.1",
        instructions="""You are a GCP bigquery generator that converts business language rules that needs to be converted into a query.
                        The query should contain generic placeholders in the format -  <schema>, <table> and <column>
                        The query should provide two aggregate values - 'total_rows' and 'valid_rows' that provide total rows present in the table and the number of rows that pass the business rule criteria
                        Example - user message- 'The field should not be null or blank' 
                        Output Text Format - SELECT COUNT(*) AS total_rows, COUNTIF(<column> IS NOT NULL AND TRIM(<column>) != '') AS valid_rows FROM <schema>.<table>""",
        input=business_rules,
        text_format=QueryModel
    )

    return response.output_parsed.query


class QueryModel(BaseModel):
    query: str = Field(description='The query that is generated against the english language rule')


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


def publish_queries_in_collibra():
    data_element_response = find_asset(collibra_config['data_element_domain_id'],
                                       collibra_config['data_element_asset_type_id'])

    for each_de in data_element_response:
        de_id = each_de.get('id')
        de_rule_relations = find_relations_by_source(de_id, collibra_config[
            'data_element_rule_specification_relation_type_id'])

        if len(de_rule_relations) > 0:
            column_de_relation = \
            find_relations_by_target(de_id, collibra_config['column_data_element_relation_type_id'])[0]
            source_details = column_de_relation.get('source').get('name').split('.')
            schema_name = source_details[0]
            table_name = source_details[1]
            column_name = source_details[2]

            for each_relation in de_rule_relations:
                related_rule_id = each_relation.get('target').get('id')
                rule_statement = find_asset_attribute_value(related_rule_id,
                                                            collibra_config['rule_statement_attribute_type_id'])
                query = generate_query(rule_statement).replace('<schema>', schema_name).replace('<table>',
                                                                                                table_name).replace(
                    '<column>', column_name)
                add_attribute(related_rule_id, collibra_config['technical_rule_attribute_type_id'], query)


publish_queries_in_collibra()














