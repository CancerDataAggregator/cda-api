from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)


################################ MATCH_ALL ################################
def test_match_all_two_filters_no_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0

def test_match_all_two_filters_one_result():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1", "subject_id_alias < 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 1

def test_match_all_foreign_column_filter():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["sex like m*"]},
        params={'limit':10}
    )
    assert response.status_code == 200 # Assert the request was successful
    assert len(response.json()['result']) > 1 # Assert the request returned some data (as expected)
    assert 'sex' in response.json()['result'][0].keys() # Assert the filter column was automatically added to results




################################ MATCH_SOME ################################
def test_match_some_one_filter_one_result():
    response = client.post(
        "/data/subject",
        json={"MATCH_SOME": ["subject_id_alias = 1"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 1

def test_match_some_two_filters_two_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 2

def test_match_some_foreign_column_filter():
    response = client.post(
        "/data/subject",
        json={"MATCH_SOME": ["sex like m*"]},
        params={'limit':10}
    )
    assert response.status_code == 200 # Assert the request was successful
    assert len(response.json()['result']) > 1 # Assert the request returned some data (as expected)
    assert 'sex' in response.json()['result'][0].keys() # Assert the filter column was automatically added to results




################################ MATCH_[ALL/SOME] Expected Interactions ################################
def test_match_all_and_match_some_single_filter_each_no_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1"],
              "MATCH_SOME": ["subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0

def test_match_all_and_match_some_no_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias > 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0

def test_match_all_and_match_some_one_result():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 1

def test_match_all_and_match_some_two_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias <= 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 2




################################ ADD_COLUMNS ################################
def test_add_columns_basic_functionality():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias <= 10"],
              "ADD_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1
    assert 'sex' in response.json()['result'][0].keys()

def test_add_columns_multiple_from_same_source_table():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", 'diagnosis']},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1
    assert 'sex' in response.json()['result'][0].keys()
    assert 'diagnosis' in response.json()['result'][0].keys()

def test_add_columns_multiple_from_varied_source_tables():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", 'file_type']},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1
    assert 'sex' in response.json()['result'][0].keys()
    assert 'file_type' in response.json()['result'][0].keys()

def test_add_columns_from_current_endpoint_table():
    response_no_add = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'limit':10}
    )
    response_add = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["subject_id"]},
        params={'limit':10}
    )
    assert response_no_add.status_code == 200
    assert response_add.status_code == 200
    # The keys should be identical since subject_id is included by default
    assert response_no_add.json()['result'][0].keys() == response_add.json()['result'][0].keys()

def test_add_columns_already_in_filter():
    response_no_add = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["sex like m*"]},
        params={'limit':10}
    )
    response_add = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["sex like m*"],
              "ADD_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response_no_add.status_code == 200
    assert response_add.status_code == 200
    # The keys should be identical since the sex column is added to the result by default
    assert response_no_add.json()['result'][0].keys() == response_add.json()['result'][0].keys()

def test_add_columns_unknown_column():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["UNKNOWN"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "ColumnNotFound"

def test_add_columns_foreign_array():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1
    assert isinstance(response.json()['result'][0]['sex'], list) # Verify the results of the sex column are returned in an array

def test_add_columns_table_dot_star():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["observation.*"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1
    assert 'sex' in response.json()['result'][0].keys()
    assert 'diagnosis' in response.json()['result'][0].keys()




################################ EXCLUDE_COLUMNS ################################
def test_exclude_columns_from_current_endpoint_table():
    response_no_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'limit':10}
    )
    response_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "EXCLUDE_COLUMNS": ["subject_id"]},
        params={'limit':10}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should not be identical since subject_id should be excluded from the results
    assert response_no_exclude.json()['result'][0].keys() != response_exclude.json()['result'][0].keys()
    assert 'subject_id' not in response_exclude.json()['result'][0].keys()

def test_exclude_columns_from_foreign_table():
    response_no_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'limit':10}
    )
    response_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "EXCLUDE_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should be identical since sex isn't returned by default anyway
    assert response_no_exclude.json()['result'][0].keys() == response_exclude.json()['result'][0].keys()

def test_exclude_columns_from_filter():
    response_no_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'limit':10}
    )
    response_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10", "sex like m*"],
              "EXCLUDE_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should be identical since sex is removed from the results. Exclude overrides this default behavior
    assert response_no_exclude.json()['result'][0].keys() == response_exclude.json()['result'][0].keys()




################################ [ADD/EXCLUDE]_COLUMNS Expected Interactions ################################
def test_add_and_exclude_columns_same_column():
    response_no_add_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'limit':10}
    )
    response_add_exclude = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"],
              "EXCLUDE_COLUMNS": ["sex"]},
        params={'limit':10}
    )
    assert response_no_add_exclude.status_code == 200
    assert response_add_exclude.status_code == 200
    # The keys should be identical since sex is added and removed. Removal always takes priority
    assert response_no_add_exclude.json()['result'][0].keys() == response_add_exclude.json()['result'][0].keys()

def test_add_and_exclude_columns_different_columns():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"],
              "EXCLUDE_COLUMNS": ["subject_id"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'sex' in response.json()['result'][0].keys()
    assert 'subject_id' not in response.json()['result'][0].keys() 




################################ COLLATE_RESULTS ################################
def test_collate_results_single_add_column():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The sex column should be returned in a nested list of dictionaries for each row in a column named "observation_columns"
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()

def test_collate_results_filter_column():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10", 'sex like m*'],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The sex column, used as a filter, should be automatically added to the "ADD_COLUMNS" list therefore we should see the same behavior
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()

def test_collate_results_add_single_table():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["observation.*"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The keys should be identical since sex is added and removed. Removal always takes priority
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'diagnosis' in response.json()['result'][0]['observation_columns'][0].keys()
    

def test_collate_results_multiple_add_columns_from_same_table():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", "diagnosis"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # Both "sex" and "diagnosis" should be returned in "observation_columns"
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'diagnosis' in response.json()['result'][0]['observation_columns'][0].keys()

def test_collate_results_multiple_add_columns_from_two_tables():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", "file_type"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # Both "sex" should be returned in "observation_columns" and "file_type" should be returned in "file_columns"
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'file_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['file_columns'], list)
    assert isinstance(response.json()['result'][0]['file_columns'][0], dict)
    assert 'file_type' in response.json()['result'][0]['file_columns'][0].keys()

def test_collate_results_add_multiple_tables():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["observation.*", "file.*"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The keys should be identical since sex is added and removed. Removal always takes priority
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'diagnosis' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'file_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['file_columns'], list)
    assert isinstance(response.json()['result'][0]['file_columns'][0], dict)
    assert 'file_type' in response.json()['result'][0]['file_columns'][0].keys()
    assert 'size' in response.json()['result'][0]['file_columns'][0].keys()




################################ EXTERNAL_REFERENCE ################################
def test_external_reference_popultated_result():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id like APOLLO.AP-2*"], # both subject's with id like "APOLLO.AP-2*" appear to have external_references
              "EXTERNAL_REFERENCE": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The external_reference_columns should contain a list of dictonaries for the rows of related external references
    assert 'external_reference_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['external_reference_columns'], list)
    assert isinstance(response.json()['result'][0]['external_reference_columns'][0], dict)
    assert 'external_reference_type' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_short_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'last_updated' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'uri' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_description' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'source_short_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'source_url' in response.json()['result'][0]['external_reference_columns'][0].keys()

def test_external_reference_null_result():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id like ALCHEMIST.ALCH-ABB*"], # both subject's with id like "ALCHEMIST.ALCH-ABB*" appear to not have external_references
              "EXTERNAL_REFERENCE": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    # The external_reference_columns should be empty if there are no related external references
    assert 'external_reference_columns' in response.json()['result'][0].keys()
    assert response.json()['result'][0]['external_reference_columns'] == None


################################ SEARCH_LIST ################################
def test_search_list_single_keyword_match():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lung'] # 'lung' will match a keyword so a text_search ts_query will be not initiated
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' not in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_data_file_endpoint():
    response = client.post(
        "/data/file",
        json={
              "SEARCH_LIST": ['lung'] # 'lung' will match a keyword so a text_search ts_query will be not initiated
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' not in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_two_keyword_matches():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lung', 'male'] 
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' not in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_single_ts_vector_match():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lungs'] # 'lungs' won't match a keyword so a text_search ts_query will be initiated
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' not in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_two_ts_vector_matches():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lungs', 'males']
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' not in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_wildcard_keyword_match():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['male*'] # 'male*' matches multiple keywords and therefore should not create a ts_vector
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' not in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_no_matching_wildcard_keyword_error():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lungs*'] # 'lungs*' won't match any keywords and therefore will throw an error
              },
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == 'InvalidSearchError'
    assert response.json()['message'] == 'Term with wildcard: \"lungs*\" yielded no results'


def test_search_list_keyword_and_ts_vector_matches():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['lung', 'males'] # 'lung' match a keyword but 'males' won't so a both the keyword preselect and the text_search ts_query will be initiated
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' in response.json()['query_sql']
    assert len(response.json()['result']) >= 1

def test_search_list_file_only_match():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['cortex of kidney'] # 'cortex of kidney' will match in file keywords but not in subject keywords only the file keyword preselect will be created 
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert 'unified_keyword_preselect' not in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' not in response.json()['query_sql']
    assert 'subject_exclusive_keywords_preselect' not in response.json()['query_sql']
    assert 'file_exclusive_keywords_preselect' in response.json()['query_sql']

def test_search_list_no_matching_keyword_or_ts_query():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ['BADTERM'] # This term should yield an empty result
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0



################################ All Together Now ################################
def test_request_body_data_subject():
    response = client.post(
        "/data/subject",
        json={
              "SEARCH_LIST": ["lung*", "males"],
              "MATCH_ALL": ["subject_id like APOLLO.AP-2*", "species is not null"],
              "MATCH_SOME": ["diagnosis like a*", "sex like f*"],
              "ADD_COLUMNS": ["observation.*", "file.*"],
              "EXCLUDE_COLUMNS": ["ethnicity", "diagnosis"],
              "COLLATE_RESULTS": True,
              "EXTERNAL_REFERENCE": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result'][0]) >= 1
    assert "ethnicity" not in response.json()['result'][0].keys()
    assert 'observation_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['observation_columns'], list)
    assert isinstance(response.json()['result'][0]['observation_columns'][0], dict)
    assert 'sex' in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'diagnosis' not in response.json()['result'][0]['observation_columns'][0].keys()
    assert 'file_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['file_columns'], list)
    assert isinstance(response.json()['result'][0]['file_columns'][0], dict)
    assert 'file_type' in response.json()['result'][0]['file_columns'][0].keys()
    assert 'size' in response.json()['result'][0]['file_columns'][0].keys()
    assert 'unified_keyword_preselect' in response.json()['query_sql']
    assert 'unmatched_keyword_text_search_preselect' in response.json()['query_sql']
    assert 'external_reference_columns' in response.json()['result'][0].keys()
    assert isinstance(response.json()['result'][0]['external_reference_columns'], list)
    assert isinstance(response.json()['result'][0]['external_reference_columns'][0], dict)
    assert 'external_reference_type' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_short_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'last_updated' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'uri' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'external_reference_description' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'source_short_name' in response.json()['result'][0]['external_reference_columns'][0].keys()
    assert 'source_url' in response.json()['result'][0]['external_reference_columns'][0].keys()