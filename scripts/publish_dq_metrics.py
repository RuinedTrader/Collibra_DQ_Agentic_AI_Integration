import scripts.publish_query as collibra_util
import scripts.gcp_big_query_call as bq

def generate_dq_metric():
    data_element_response = collibra_util.find_asset(collibra_util.collibra_config['data_element_domain_id'],collibra_util.collibra_config['data_element_asset_type_id'])
    for each_de in data_element_response:
        de_id = each_de.get('id')
        de_rule_relations = collibra_util.find_relations_by_source(de_id,collibra_util.collibra_config['data_element_rule_specification_relation_type_id'])

        for each_relation in de_rule_relations:
            related_rule_id = each_relation.get('target').get('id')
            related_rule_name = each_relation.get('target').get('name')
            query = collibra_util.find_asset_attribute_value(related_rule_id,collibra_util.collibra_config['technical_rule_attribute_type_id'])

            if query is not None and len(query.strip()) != 0:
                output = list(bq.run_query(query))[0]
                loaded_rows = output[0]
                passed_rows = output[1]

                rule_dqm_relations = collibra_util.find_relations_by_source(related_rule_id,collibra_util.collibra_config['rule_dqm_relation_type_id'])

                if len(rule_dqm_relations)>0:
                    dqm_id = rule_dqm_relations[0].get('target').get('id')
                else:
                    dqm_name = 'DQM_'+related_rule_name
                    dqm_id = collibra_util.add_asset(dqm_name,dqm_name,collibra_util.collibra_config['data_quality_metric_asset_type_id'],collibra_util.collibra_config['dqm_domain_id'],collibra_util.collibra_config['approved_status_id'])
                    collibra_util.add_relation(related_rule_id, dqm_id,collibra_util.collibra_config['rule_dqm_relation_type_id'])

                collibra_util.add_attribute(dqm_id,collibra_util.collibra_config['threshold_attribute_type_id'],collibra_util.collibra_config['threshold_attribute_value'])
                collibra_util.add_attribute(dqm_id,collibra_util.collibra_config['loaded_rows_attribute_type_id'],loaded_rows)
                collibra_util.add_attribute(dqm_id,collibra_util.collibra_config['rows_passed_attribute_type_id'],passed_rows)
                collibra_util.add_attribute(dqm_id,collibra_util.collibra_config['rows_failed_attribute_type_id'],loaded_rows-passed_rows)

                passing_fraction = round((passed_rows/loaded_rows)*100,2)
                collibra_util.add_attribute(dqm_id,collibra_util.collibra_config['passing_fraction_attribute_type_id'],passing_fraction)
                collibra_util.add_attribute(dqm_id, collibra_util.collibra_config['result_attribute_type_id'],passing_fraction>collibra_util.collibra_config['threshold_attribute_value'])

generate_dq_metric()




