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




################################ All Together Now ################################
def test_request_body_data_subject():
    response = client.post(
        "/data/subject",
        json={
              "MATCH_ALL": ["subject_id_alias < 500", "species is not null"],
              "MATCH_SOME": ["diagnosis like a*", "sex like f*"],
              "ADD_COLUMNS": ["observation.*", "file.*"],
              "EXCLUDE_COLUMNS": ["ethnicity", "diagnosis"],
              "COLLATE_RESULTS": True
              },
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result'][0]) > 1
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