import json
import random

import scripts.publish_query as collibra_util
import scripts.openai_api_call as dq_agent
import yaml
from datetime import datetime


def get_date_from_epoch(epoch_time):
    formatted_date = datetime.fromtimestamp(timestamp=epoch_time/1000).strftime("%Y/%m/%d")
    return formatted_date


with open('../configurations/collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file,Loader=yaml.FullLoader)

dq_metric_response = collibra_util.find_asset(collibra_config['dqm_domain_id'],
                         collibra_config['data_quality_metric_asset_type_id'])



def generate_data_concern_and_fix():
    for each_dqm in dq_metric_response:
        dqm_id = each_dqm.get('id')

        passing_flag = collibra_util.find_asset_attribute_value(dqm_id,collibra_config['result_attribute_type_id']) #Fetch result value

        if not passing_flag:
            dq_score = collibra_util.find_asset_attribute_value(dqm_id,collibra_config['passing_fraction_attribute_type_id'])
            rows_passed = collibra_util.find_asset_attribute_value(dqm_id, collibra_config['rows_passed_attribute_type_id'])
            rows_failed = collibra_util.find_asset_attribute_value(dqm_id, collibra_config['rows_failed_attribute_type_id'])
            threshold = collibra_util.find_asset_attribute_value(dqm_id, collibra_config['threshold_attribute_type_id'])

            rule_dqm_rel =  collibra_util.find_relations_by_target(dqm_id,collibra_config['rule_dqm_relation_type_id'])[0]
            rule_id = rule_dqm_rel.get('source').get('id')
            query = collibra_util.find_asset_attribute_value(rule_id,collibra_config['technical_rule_attribute_type_id'])
            rule_statement = collibra_util.find_asset_attribute_value(rule_id,collibra_config['rule_statement_attribute_type_id'])

            de_rule_rel = collibra_util.find_relations_by_target(rule_id,collibra_config['data_element_rule_specification_relation_type_id'])[0]
            de_id = de_rule_rel.get('source').get('id')
            de_name = de_rule_rel.get('source').get('name')

            column_de_relation = collibra_util.find_relations_by_target(de_id, collibra_config['column_data_element_relation_type_id'])[0]
            source_details = column_de_relation.get('source').get('name').split('.')
            schema_name = source_details[0]
            table_name = source_details[1]
            column_name = source_details[2]


            dqm_activity = collibra_util.search_activities_for_attributes(dqm_id)

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

            fixes_response = dq_agent.generate_fix(schema_name, table_name, column_name, query, rule_statement, dq_score,
                                          rows_passed, rows_failed, threshold, passing_flag, dq_history)

            dq_issue = fixes_response.dq_issue_description
            trend_analysis = fixes_response.trend_analysis

            count = 1
            resolution=""
            for each_fix in fixes_response.fix_strategies:
                resolution =resolution+ f"<p><strong>Resolution {count} :</strong>{each_fix.fix_strategy}</p><p><strong>Query :</strong> <code>{each_fix.fix_query}</code></p><p><strong>Confidence :</strong>{each_fix.confidence_score}%</p><br>"
                count+=1

            di_dqm_relations = collibra_util.find_relations_by_target(dqm_id, collibra_util.collibra_config['data_concern_dqm_relation_type_id'])

            if len(di_dqm_relations) != 0:
                di_id = di_dqm_relations[0].get('source').get('id')
            else:
                di_name = 'Data_Issue_' + de_name + '_' + str(random.randint(300000, 400000))
                di_id = collibra_util.add_asset(di_name, di_name, collibra_config['data_issue_asset_type_id'], collibra_config['data_issue_domain_id'],collibra_config['approved_status_id'])
                collibra_util.add_relation(de_id, di_id, collibra_config['data_element_data_issue_relation_type_id'])
                collibra_util.add_relation(di_id, dqm_id, collibra_config['data_concern_dqm_relation_type_id'])


            collibra_util.add_attribute(di_id,collibra_config['description_attribute_type_id'],dq_issue)
            collibra_util.add_attribute(di_id, collibra_config['trend_analysis_attribute_type_id'], trend_analysis)
            collibra_util.add_attribute(di_id, collibra_config['resolution_attribute_type_id'], resolution)

generate_data_concern_and_fix()














