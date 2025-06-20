import json

import yaml
import requests
from requests.auth import HTTPBasicAuth
import scripts.openai_api_call as ai_agent
import dotenv
import os


with open('../configurations/collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file,Loader=yaml.FullLoader)



dotenv.load_dotenv('../keys/.env')
collibra_host_endpoint = collibra_config['base_api_endpoint']
username = os.getenv('COLLIBRA_USERNAME')
password = os.getenv('COLLIBRA_PASSWORD')
auth = HTTPBasicAuth(username,password)



def find_asset(domain_id, asset_type_id):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'limit': 1000000, 'domainId': domain_id, 'typeId': asset_type_id}
    return requests.get(url=url,headers=headers,params=params,auth=auth).json().get('results')


def find_relations_by_source(source_asset_id,relation_type_id):
    url = collibra_host_endpoint + collibra_config["relation_endpoint"]
    headers = {'accept':'application/json'}
    params = {'relationTypeId':relation_type_id,'sourceId': source_asset_id}
    return requests.get(url=url,headers=headers,params=params,auth=auth).json().get('results')

def find_relations_by_target(target_asset_id,relation_type_id):
    url = collibra_host_endpoint + collibra_config["relation_endpoint"]
    headers = {'accept':'application/json'}
    params = {'relationTypeId':relation_type_id,'targetId': target_asset_id}
    return requests.get(url=url,headers=headers,params=params,auth=auth).json().get('results')


def find_asset_attribute_value(asset_id, attribute_type_id):
    url = collibra_host_endpoint + collibra_config["attribute_endpoint"]
    headers = {'accept': 'application/json'}
    params = {'typeIds': attribute_type_id, 'assetId': asset_id}
    attribute =  requests.get(url=url, headers=headers, params=params, auth=auth).json().get('results')
    if len(attribute) != 0:
        return attribute[0].get('value')
    return None

def add_attribute(asset_id, attribute_type_id, value):
    url = collibra_host_endpoint + collibra_config["asset_endpoint"]+f'/{asset_id}/attributes'
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
    params = {'contextId': context_id, 'categories': 'ATTRIBUTE','resourceDiscriminators':'NumericAttribute','activityType':'ADD'}

    return requests.get(url, params=params, headers=headers,auth=auth).json().get('results')



def publish_queries_in_collibra():
    data_element_response = find_asset(collibra_config['data_element_domain_id'],collibra_config['data_element_asset_type_id'])

    for each_de in data_element_response:
        de_id = each_de.get('id')
        de_rule_relations = find_relations_by_source(de_id,collibra_config['data_element_rule_specification_relation_type_id'])

        if len(de_rule_relations) > 0:
            column_de_relation = find_relations_by_target(de_id,collibra_config['column_data_element_relation_type_id'])[0]
            source_details = column_de_relation.get('source').get('name').split('.')
            schema_name = source_details[0]
            table_name = source_details[1]
            column_name = source_details[2]

            for each_relation in de_rule_relations:
                related_rule_id = each_relation.get('target').get('id')
                rule_statement = find_asset_attribute_value(related_rule_id,collibra_config['rule_statement_attribute_type_id'])
                query = ai_agent.generate_query(rule_statement).replace('<schema>',schema_name).replace('<table>',table_name).replace('<column>',column_name)
                add_attribute(related_rule_id,collibra_config['technical_rule_attribute_type_id'],query)


publish_queries_in_collibra()

# from datetime import datetime
# def get_date_from_epoch(epoch_time):
#     formatted_date = datetime.fromtimestamp(timestamp=epoch_time / 1000).strftime("%Y/%m/%d")
#     return formatted_date
#
#
# dqm_activity = search_activities_for_attributes('01976254-d9d6-7736-909a-bc799e425081')
# dq_history = {}
# for each_activity in dqm_activity:
#     description = json.loads(each_activity['description'])
#     if description.get('field') != 'Threshold' and description.get('field') != 'Loaded Rows':
#         epoch_val = each_activity['timestamp']
#         activity_datetime = get_date_from_epoch(epoch_val)
#         field = description['field']
#         value = description['new']['name']
#         current_data_map = {field: value}
#         if dq_history.get(activity_datetime) is not None:
#             existing_data_list = dq_history[activity_datetime]
#             if current_data_map not in existing_data_list:
#                 existing_data_list.append(current_data_map)
#         else:
#             dq_history[activity_datetime] = [current_data_map]
#
# print(json.dumps(dq_history,indent=4))






