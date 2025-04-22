from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)


################################ summary/subject testing ################################
def test_summary_subject_endpoint_query_generation():
    response = client.post(
        "/summary/subject",
        json={"MATCH_ALL": ["subject_id_alias < 0"]},
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
        json={"MATCH_ALL": ["file_id_alias < 0"]},
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
