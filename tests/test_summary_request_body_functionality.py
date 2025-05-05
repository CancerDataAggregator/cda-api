from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)


################################ MATCH_ALL ################################
def test_match_all_two_filters_no_results():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert isinstance(response.json()['result'], list)
    assert isinstance(response.json()['result'][0], dict)
    assert 'total_count' in response.json()['result'][0].keys()
    assert response.json()['result'][0]['total_count'] == 0

def test_match_all_two_filters_one_result():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1", "subject_id_alias < 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 1

def test_match_all_foreign_column_filter():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["sex like m*"]}
    )
    assert response.status_code == 200 # Assert the request was successful
    assert response.json()['result'][0]['total_count'] > 1
    assert 'sex_summary' in response.json()['result'][0].keys() # Assert the filter column was automatically added to results




################################ MATCH_SOME ################################
def test_match_some_one_filter_one_result():
    response = client.post(
        "/summary/subject",
        json={"MATCH_SOME": ["subject_id_alias = 1"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 1

def test_match_some_two_filters_two_results():
    response = client.post(
        "/summary/subject",
        json={"MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 2

def test_match_some_foreign_column_filter():
    response = client.post(
        "/summary/subject",
        json={"MATCH_SOME": ["sex like m*"]}
    )
    assert response.status_code == 200 # Assert the request was successful
    assert response.json()['result'][0]['total_count'] > 1 # Assert the request returned some data (as expected)
    assert 'sex_summary' in response.json()['result'][0].keys() # Assert the filter column was automatically added to results




################################ MATCH_[ALL/SOME] Expected Interactions ################################
def test_match_all_and_match_some_single_filter_each_no_results():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias = 1"],
              "MATCH_SOME": ["subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 0

def test_match_all_and_match_some_no_results():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias > 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 0

def test_match_all_and_match_some_one_result():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 1

def test_match_all_and_match_some_two_results():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias <= 10"],
              "MATCH_SOME": ["subject_id_alias = 1", "subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] == 2




################################ ADD_COLUMNS ################################
def test_add_columns_basic_functionality():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias <= 10"],
              "ADD_COLUMNS": ["sex"]}
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] > 1
    assert 'sex_summary' in response.json()['result'][0].keys()

def test_add_columns_multiple_from_same_source_table():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", 'diagnosis']}
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] > 1
    assert 'sex_summary' in response.json()['result'][0].keys()
    assert 'diagnosis_summary' in response.json()['result'][0].keys()

def test_add_columns_multiple_from_varied_source_tables():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex", 'file_type']}
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] > 1
    assert 'sex_summary' in response.json()['result'][0].keys()
    assert 'file_type_summary' in response.json()['result'][0].keys()

def test_add_columns_from_current_endpoint_table():
    response_no_add = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]}
    )
    response_add = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["subject_id"]}
    )
    assert response_no_add.status_code == 200
    assert response_add.status_code == 200
    # The keys should be identical since subject_id is included by default
    assert response_no_add.json()['result'][0].keys() == response_add.json()['result'][0].keys()

def test_add_columns_already_in_filter():
    response_no_add = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["sex like m*"]}
    )
    response_add = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["sex like m*"],
              "ADD_COLUMNS": ["sex"]}
    )
    assert response_no_add.status_code == 200
    assert response_add.status_code == 200
    # The keys should be identical since the sex column is added to the result by default
    assert response_no_add.json()['result'][0].keys() == response_add.json()['result'][0].keys()

def test_add_columns_unknown_column():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["UNKNOWN"]}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "ColumnNotFound"


def test_add_columns_table_dot_star():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["observation.*"]}
    )
    assert response.status_code == 200
    assert response.json()['result'][0]['total_count'] > 1
    assert 'sex_summary' in response.json()['result'][0].keys()
    assert 'diagnosis_summary' in response.json()['result'][0].keys()




################################ EXCLUDE_COLUMNS ################################
def test_exclude_columns_from_current_endpoint_table():
    response_no_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]}
    )
    response_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "EXCLUDE_COLUMNS": ["species"]}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should not be identical since subject_id should be excluded from the results
    assert response_no_exclude.json()['result'][0].keys() != response_exclude.json()['result'][0].keys()
    assert 'species_summary' not in response_exclude.json()['result'][0].keys()

def test_exclude_columns_from_foreign_table():
    response_no_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]}
    )
    response_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "EXCLUDE_COLUMNS": ["sex"]}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should be identical since sex isn't returned by default anyway
    assert response_no_exclude.json()['result'][0].keys() == response_exclude.json()['result'][0].keys()

def test_exclude_columns_from_filter():
    response_no_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]}
    )
    response_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10", "sex like m*"],
              "EXCLUDE_COLUMNS": ["sex"]}
    )
    assert response_no_exclude.status_code == 200
    assert response_exclude.status_code == 200
    # The keys should be identical since sex is removed from the results. Exclude overrides this default behavior
    assert response_no_exclude.json()['result'][0].keys() == response_exclude.json()['result'][0].keys()




################################ [ADD/EXCLUDE]_COLUMNS Expected Interactions ################################
def test_add_and_exclude_columns_same_column():
    response_no_add_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]}
    )
    response_add_exclude = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"],
              "EXCLUDE_COLUMNS": ["sex"]}
    )
    assert response_no_add_exclude.status_code == 200
    assert response_add_exclude.status_code == 200
    # The keys should be identical since sex is added and removed. Removal always takes priority
    assert response_no_add_exclude.json()['result'][0].keys() == response_add_exclude.json()['result'][0].keys()

def test_add_and_exclude_columns_different_columns():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"],
              "ADD_COLUMNS": ["sex"],
              "EXCLUDE_COLUMNS": ["species"]}
    )
    assert response.status_code == 200
    assert 'sex_summary' in response.json()['result'][0].keys()
    assert 'species_summary' not in response.json()['result'][0].keys() 




