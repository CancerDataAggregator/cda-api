from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)


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


def test_release_metadata_endpoint():
    response = client.get("/release_metadata")
    assert response.status_code == 200


def test_columns_endpoint():
    response = client.get("/columns")
    assert response.status_code == 200
