import functions_framework
import yaml
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery

with open('collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file, Loader=yaml.FullLoader)

collibra_host_endpoint = collibra_config['base_api_endpoint']
username = collibra_config['collibra_username']
password = collibra_config['collibra_password']

auth = HTTPBasicAuth(username,password)
client = bigquery.Client()


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


def run_query(query):
    """Runs the provided query in GCP Bigquery instance"""
    query_job = client.query(query)  # API request
    row = query_job.result()  # Waits for query to finish
    return row


@functions_framework.http
def generate_dq_metric(request):
    rule_spec_response = find_asset(collibra_config['rule_spec_domain_id'],collibra_config['rule_specification_asset_type_id'])
    for each_rule in rule_spec_response:
        rule_id = each_rule.get('id')
        rule_name = each_rule.get('name')
        query = find_asset_attribute_value(rule_id, collibra_config['technical_rule_attribute_type_id'])

        if query is not None and len(query.strip()) != 0:
            output = list(run_query(query))[0]
            loaded_rows = output[0]
            passed_rows = output[1]

            rule_dqm_relations = find_relations_by_source(rule_id,collibra_config['rule_dqm_relation_type_id'])

            if len(rule_dqm_relations) > 0:
                dqm_id = rule_dqm_relations[0].get('target').get('id')
            else:
                dqm_name = 'DQM_' + rule_name
                dqm_id = add_asset(dqm_name, dqm_name, collibra_config['data_quality_metric_asset_type_id'],
                                   collibra_config['dqm_domain_id'], collibra_config['approved_status_id'])
                add_relation(rule_id, dqm_id, collibra_config['rule_dqm_relation_type_id'])

            add_attribute(dqm_id, collibra_config['threshold_attribute_type_id'],
                          collibra_config['threshold_attribute_value'])
            add_attribute(dqm_id, collibra_config['loaded_rows_attribute_type_id'], loaded_rows)
            add_attribute(dqm_id, collibra_config['rows_passed_attribute_type_id'], passed_rows)
            add_attribute(dqm_id, collibra_config['rows_failed_attribute_type_id'], loaded_rows - passed_rows)

            passing_fraction = round((passed_rows / loaded_rows) * 100, 2)
            add_attribute(dqm_id, collibra_config['passing_fraction_attribute_type_id'], passing_fraction)
            add_attribute(dqm_id, collibra_config['result_attribute_type_id'],
                          passing_fraction > collibra_config['threshold_attribute_value'])

    return 'Run Complete'