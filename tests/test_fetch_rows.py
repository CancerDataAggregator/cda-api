from fastapi.testclient import TestClient
from cda_api.db.query_builders import paged_query
from cda_api import app, ColumnNotFound

client = TestClient(app)


################################ data/subject testing ################################
def test_data_subject_endpoint_query_generation():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 0"]},
    )
    assert response.status_code == 200
    assert response.json()['query_sql'].startswith('SELECT')


def test_data_subject_endpoint_limit():
    response = client.post(
        "/data/subject",
        json={},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_data_subject_endpoint_offset_and_limit():
    response = client.post(
        "/data/subject",
        json={},
        params={'offset': 10, 'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10


def test_data_subject_endpoint_offset_too_big():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
        params={'offset':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0


def test_data_subject_endpoint_column_not_found():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {'detail': "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 404
    assert response.json() == expected_response_json


################################ data/file testing ################################
def test_data_file_endpoint_query_generation():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["file_id_alias < 0"]},
    )
    assert response.status_code == 200
    assert response.json()['query_sql'].startswith('SELECT')


def test_data_file_endpoint_limit():
    response = client.post(
        "/data/file",
        json={},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_data_file_endpoint_offset_and_limit():
    response = client.post(
        "/data/file",
        json={},
        params={'offset': 10, 'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10


def test_data_file_endpoint_offset_too_big():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["file_id_alias < 10"]},
        params={'offset':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 0


def test_data_file_endpoint_column_not_found():
    response = client.post(
        "/data/file",
        json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
    )
    expected_response_json = {'detail': "Column Not Found: FAKE_COLUMN\n'FAKE_COLUMN'"}
    assert response.status_code == 404
    assert response.json() == expected_response_json