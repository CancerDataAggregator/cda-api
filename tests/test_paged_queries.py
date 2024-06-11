from fastapi.testclient import TestClient
from cda_api.db.query_builders import paged_query
from cda_api import app, ColumnNotFound

client = TestClient(app)


def test_data_observation_endpoint_simple():
    response = client.post(
        "/data/observation",
        json={"MATCH_ALL": ["sex like m%"]},
    )
    s = "SELECT observation.id, observation.integer_id_alias, observation.vital_status, observation.sex, observation.year_of_diagnosis, observation.diagnosis, observation.morphology, observation.grade, observation.stage, observation.tissue_or_organ_of_origin, observation.data_at_cds, observation.data_at_gdc, observation.data_at_icdc, observation.data_at_idc, observation.data_at_pdc, observation.data_source_count FROM observation WHERE coalesce(upper(observation.sex), '') LIKE upper('m%')"
    assert response.status_code == 200
    assert response.json()['query_sql'] == s


def test_data_observation_endpoint_column_not_found():
    try: 
        response = client.post(
            "/data/observation",
            json={"MATCH_ALL": ["FAKE_COLUMN = 42"]},
        )
        assert response.status_code == 200
    except ColumnNotFound as e:
        assert True
    except Exception as e:
        raise e