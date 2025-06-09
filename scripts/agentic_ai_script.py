import scripts.publish_query as collibra_util
import scripts.openai_api_call as dq_agent


# step 1: get the threshold values from DQ metrics assets

dq_metric_response = collibra_util.find_asset('0196e992-e49f-743c-ac4f-84f4ab791323',
                         '00000000-0000-0000-0000-000000031107')

for each_dqm in dq_metric_response:
    dqm_id = each_dqm.get('id')

    passing_flag = collibra_util.find_asset_attribute_value(dqm_id,'00000000-0000-0000-0000-000000000238') #Fetch result value

    if not passing_flag: # step 2: if the threshold value of the DQ metric is not met, fetch table/column
        print(each_dqm.get('name'))
        dq_score = collibra_util.find_asset_attribute_value(dqm_id,'00000000-0000-0000-0000-000000000240')

       # get rule from dqm
        rule_dqm_rel =  collibra_util.find_relations_by_target(dqm_id,'0196e9c4-799c-745e-9cca-076fcc8ba19c')[0]
        rule_id = rule_dqm_rel.get('source').get('id')
        query = collibra_util.find_asset_attribute_value(rule_id,'01970fdb-6de1-7b32-8912-ace368669b9d')
        rule_statement = collibra_util.find_asset_attribute_value(rule_id,'0196e8b4-859a-798b-ae28-3438aa614614')

        # get DE from rule
        de_rule_rel = collibra_util.find_relations_by_target(rule_id,'0196e8b9-47cf-787d-99f5-668552f38515')[0]
        de_id = de_rule_rel.get('source').get('id')

        #get column/table
        column_de_relation = collibra_util.find_relations_by_target(de_id, '626267ca-7b75-4a47-977a-7bc6a743ccab')[0]
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


