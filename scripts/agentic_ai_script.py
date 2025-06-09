import scripts.publish_query as collibra_util
import scripts.openai_api_call as dq_agent
import yaml

with open('../configurations/collibra_config.yml', mode='r') as config_file:
    collibra_config = yaml.load(config_file,Loader=yaml.FullLoader)


# step 1: get the failed DQ metrics assets

dq_metric_response = collibra_util.find_asset(collibra_config['dqm_domain_id'],
                         collibra_config['data_quality_metric_asset_type_id'])

for each_dqm in dq_metric_response:
    dqm_id = each_dqm.get('id')

    passing_flag = collibra_util.find_asset_attribute_value(dqm_id,collibra_config['result_attribute_type_id']) #Fetch result value

    if not passing_flag: # step 2: if the threshold value of the DQ metric is not met, fetch table/column
        print(each_dqm.get('name'))
        dq_score = collibra_util.find_asset_attribute_value(dqm_id,collibra_config['passing_fraction_attribute_type_id'])

       # get rule from dqm
        rule_dqm_rel =  collibra_util.find_relations_by_target(dqm_id,collibra_config['rule_dqm_relation_type_id'])[0]
        rule_id = rule_dqm_rel.get('source').get('id')
        query = collibra_util.find_asset_attribute_value(rule_id,collibra_config['technical_rule_attribute_type_id'])
        rule_statement = collibra_util.find_asset_attribute_value(rule_id,collibra_config['rule_statement_attribute_type_id'])

        # get DE from rule
        de_rule_rel = collibra_util.find_relations_by_target(rule_id,collibra_config['data_element_rule_specification_relation_type_id'])[0]
        de_id = de_rule_rel.get('source').get('id')

        #get column/table
        column_de_relation = collibra_util.find_relations_by_target(de_id, collibra_config['column_data_element_relation_type_id'])[0]
        source_details = column_de_relation.get('source').get('name').split('.')
        schema_name = source_details[0]
        table_name = source_details[1]
        column_name = source_details[2]


        # step 3: run the agentic AI workflow script -> get suggestions on how to fix it -> provide the column/table/schema and the query that generated that result
        print('Generating fix...')
        fixes_response = dq_agent.generate_fix(schema_name,table_name,column_name,query,rule_statement,dq_score,passing_flag)

        print('Issue: '+fixes_response.dq_issue_description,end='\n\n')
        for each_fix in fixes_response.fix_strategies:
            solution = each_fix.fix_strategy
            suggested_query = each_fix.fix_query
            confidence_score = each_fix.confidence_score

            print(f"Solution: {solution}\nSuggested Query: {suggested_query}\nConfidence_score = {confidence_score}",end='\n\n')

# step 4: notify the stakeholders regarding fix and provide an option to proceed or cancel the solution-> collibra wf
# step 5: if yes, connect to gcp big query and run the DML query


