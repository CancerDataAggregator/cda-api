from fastapi.testclient import TestClient
from cda_api.db.query_builders import paged_query
from cda_api import app

client = TestClient(app)


def test_hello_endpoint():
    response = client.get("/hello")
    assert response.status_code == 200


def test_bad_endpoint():
    response = client.get("/FAKE_ENDPOINT")
    assert response.status_code == 404