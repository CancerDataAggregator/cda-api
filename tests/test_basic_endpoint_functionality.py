from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)

################################ baic functionality test ################################
def test_bad_endpoint():
    response = client.get("/FAKE_ENDPOINT")
    assert response.status_code == 404


def test_data_subject_endpoint():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 1"]},
    )
    assert response.status_code == 200


def test_data_file_endpoint():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["file_id_alias < 1"]}
    )
    assert response.status_code == 200


def test_summary_subject_endpoint():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 1"]},
    )
    assert response.status_code == 200


def test_summary_file_endpoint():
    response = client.post(
        "/summary/file",
        json={"MATCH_ALL": ["file_id_alias < 1"]},
    )
    assert response.status_code == 200

def test_column_values_endpoint():
    column = 'subject_id_alias'
    response = client.post(
        f"/column_values/{column}",
    )
    assert response.status_code == 200

def test_release_metadata_endpoint():
    response = client.get("/release_metadata")
    assert response.status_code == 200


def test_columns_endpoint():
    response = client.get("/columns")
    assert response.status_code == 200




################################ data/subject testing ################################
def test_data_subject_endpoint_query_generation():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 0"]},
    )
    assert response.status_code == 200
    assert response.json()["query_sql"].startswith("WITH subject_preselect")


def test_data_subject_endpoint_limit():
    response = client.post("/data/subject", json={"MATCH_ALL": ["subject_id_alias < 100"]}, params={"limit": 10})
    assert response.status_code == 200
    assert len(response.json()["result"]) == 10


def test_data_subject_endpoint_offset_and_limit():
    response = client.post(
        "/data/subject", json={"MATCH_ALL": ["subject_id_alias < 100"]}, params={"offset": 10, "limit": 10}
    )
    assert response.status_code == 200
    assert len(response.json()["result"]) == 10


def test_data_subject_endpoint_offset_too_big():
    response = client.post("/data/subject", json={"MATCH_ALL": ["subject_id_alias < 10"]}, params={"offset": 10})
    assert response.status_code == 200
    assert len(response.json()["result"]) == 0


def test_data_subject_endpoint_column_not_found():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {"error_type": "ColumnNotFound", "message": "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 400
    assert response.json() == expected_response_json


################################ data/file testing ################################
def test_data_file_endpoint_query_generation():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["file_id_alias < 0"]},
    )
    assert response.status_code == 200
    assert response.json()["query_sql"].startswith("WITH file_preselect")


def test_data_file_endpoint_limit():
    response = client.post("/data/file", json={"MATCH_ALL": ["file_id_alias < 100"]}, params={"limit": 10})
    assert response.status_code == 200
    assert len(response.json()["result"]) == 10


def test_data_file_endpoint_offset_and_limit():
    response = client.post("/data/file", json={"MATCH_ALL": ["file_id_alias < 100"]}, params={"offset": 10, "limit": 10})
    assert response.status_code == 200
    assert len(response.json()["result"]) == 10


def test_data_file_endpoint_offset_too_big():
    response = client.post("/data/file", json={"MATCH_ALL": ["file_id_alias < 10"]}, params={"offset": 10})
    assert response.status_code == 200
    assert len(response.json()["result"]) == 0


def test_data_file_endpoint_column_not_found():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {"error_type": "ColumnNotFound", "message": "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 400
    assert response.json() == expected_response_json




################################ summary/subject testing ################################
def test_summary_subject_endpoint_query_generation():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
    )
    assert response.status_code == 200
    assert response.json()["query_sql"].startswith("WITH")


def test_summary_subject_endpoint_column_not_found():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {"error_type": "ColumnNotFound", "message": "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 400
    assert response.json() == expected_response_json


################################ summary/file testing ################################
def test_summary_file_endpoint_query_generation():
    response = client.post(
        "/summary/file",
        json={"MATCH_ALL": ["file_id_alias < 10"]},
    )
    assert response.status_code == 200
    assert response.json()["query_sql"].startswith("WITH")


def test_summary_file_endpoint_column_not_found():
    response = client.post(
        "/summary/file",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {"error_type": "ColumnNotFound", "message": "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 400
    assert response.json() == expected_response_json


################################ column_values/column testing ################################
def test_column_values_endpoint_column_not_found():
    column = 'FAKE_COLUMN'
    response = client.post(
        f"/column_values/{column}",
    )
    expected_response_json = {"error_type": "ColumnNotFound", "message": "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 400
    assert response.json() == expected_response_json

def test_column_values_endpoint_return_structure():
    column = 'diagnosis'
    response = client.post(
        f"/column_values/{column}",
    )
    assert response.status_code == 200
    assert "result" in response.json().keys()
    assert "query_sql" in response.json().keys()
    assert "total_row_count" in response.json().keys()
    assert "next_url" in response.json().keys()
    assert isinstance(response.json()["result"], list)
    assert len(response.json()["result"]) > 1
    assert isinstance(response.json()["result"][0], dict)
    assert isinstance(response.json()["query_sql"], str)
    assert isinstance(response.json()["total_row_count"], int)
    assert isinstance(response.json()["next_url"], int) or isinstance(response.json()["next_url"], type(None))

def test_column_values_endpoint_limit():
    column = 'diagnosis'
    response = client.post(
        f"/column_values/{column}",
        params={"limit": 1}
    )
    assert response.status_code == 200
    assert len(response.json()["result"]) == 1
    assert response.json()["next_url"] != None

def test_column_values_endpoint_offset_and_limit():
    column = 'diagnosis'
    response_limit = client.post(
        f"/column_values/{column}",
        params={"limit": 1}
    )
    response_limit_offset = client.post(
        f"/column_values/{column}",
        params={"limit": 1, "offset": 1}
    )
    assert response_limit.status_code == 200
    assert response_limit_offset.status_code == 200
    assert len(response_limit_offset.json()["result"]) == 1
    assert response_limit_offset.json()["result"] != response_limit.json()["result"] # Should be different results given the offset

def test_column_values_endpoint_offset_too_big():
    column = 'sex'
    response = client.post(
        f"/column_values/{column}",
        params={"offset": 100}
    )
    assert response.status_code == 200
    assert len(response.json()["result"]) == 0 # There should be less than 100 values for sex in the data which should the yield no data


################################ /release_metadata testing ################################
def test_release_metadata_endpoint_return_structure(): # Should be a dictionary containing one key "result" which is a list of dictionaries
    response = client.get(
        f"/release_metadata",
    )
    assert response.status_code == 200
    assert "result" in response.json().keys()
    assert len(response.json().keys()) == 1
    assert isinstance(response.json()["result"], list)
    assert len(response.json()["result"]) > 1
    assert isinstance(response.json()["result"][0], dict)


################################ /columns testing ################################
def test_columns_endpoint_return_structure(): # Should be a dictionary containing one key "result" which is a list of dictionaries
    response = client.get(
        f"/columns",
    )
    assert response.status_code == 200
    assert "result" in response.json().keys()
    assert len(response.json().keys()) == 1
    assert isinstance(response.json()["result"], list)
    assert len(response.json()["result"]) > 1
    assert isinstance(response.json()["result"][0], dict)
