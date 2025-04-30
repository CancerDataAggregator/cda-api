from cda_api import app
from fastapi.testclient import TestClient

client = TestClient(app)


################################ Parsing Error testing ################################
def test_unknown_operator():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id UNKNOWN_OPERATOR 10"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert response.json()["message"] == "Parsed operator: \"UNKNOWN_OPERATOR\" is not a valid operator"

def test_less_than_or_equal_to_typo():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias =< 10"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert response.json()["message"] == "Parsed operator: \"=<\" is not a valid operator"

def test_invalid_list_operator():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < [1,2,3]"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert 'Operator must be "in" or "not in" when using a list value' in response.json()["message"]

def test_invalid_list_of_strings():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id in [human, mouse]"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert 'must be a list (ex. [1,2,3] or ["a","b","c"])' in response.json()["message"]

def test_invalid_value_for_is():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is 1"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert "Operator 'is' not compatible with value" in response.json()["message"]

def test_invalid_value_for_is_not():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is not STRING"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "ParsingError"
    assert "Operator 'is not' not compatible with value" in response.json()["message"]

################################ Numeric Column Operator testing ################################




##### Correct use
def test_numeric_column_less_than_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_numeric_column_less_than_or_equal_to_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias <= 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 11

def test_numeric_column_greater_than_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias > 10"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_numeric_column_greater_than_or_equal_to_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias >= 10"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_numeric_column_equal_to_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = 10"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 1

def test_numeric_column_not_equal_to_number():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias != 10"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 10

def test_numeric_column_in_list_of_numbers():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias in [0,1,2,3,4]"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 5

def test_numeric_column_not_in_list_of_numbers():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < 10", "subject_id_alias not in [0,1,2,3,4]"]},
    )
    assert response.status_code == 200
    assert len(response.json()['result']) == 5


def test_numeric_column_is_null():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is null"]},
    )
    assert response.status_code == 200
    # For the subject_id_alias we don't expect any results
    assert len(response.json()['result']) == 0

def test_numeric_column_is_not_null():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is not null"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result'])  > 1



##### Incorrect use (Expected to fail)
def test_numeric_column_less_than_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias < STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_less_than_or_equal_to_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias <= STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_greater_than_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias > STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_greater_than_or_equal_to_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias >= STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_equal_to_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_not_equal_to_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias != STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_in_list_of_strings_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias in ['STRING1', 'STRING2']"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_not_in_list_of_strings_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias not in ['STRING1', 'STRING2']"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"


def test_numeric_column_is_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"


def test_numeric_column_is_not_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias is not false"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_equals_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias = true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_like_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias like 10"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_like_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias like STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_numeric_column_like_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_id_alias like true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"






################################ String Column Operator testing ################################

##### Correct use
def test_string_column_equal_to_string():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = human"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_equal_to_string_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = HuMaN"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_not_equal_to_string():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species != human"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_not_equal_to_string_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species != HuMaN"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_equal_to_string_with_wildcard():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = h*"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    # This should result in no responses since wildcards only work in likes and there are no values with wildcards in the data
    assert len(response.json()['result']) == 0

def test_string_column_like_string_with_wildcard():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species like h*"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_like_string_with_alternative_wildcard():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species like h%"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_like_string_with_wildcard_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species like HuM*"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_like_string_with_no_wildcard_but_expected_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species like human"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1


def test_string_column_like_string_with_no_wildcard_and_no_expected_results():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species like h"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    # Not expecting results since 'like h' is looking for species = 'h' which there are none in the data
    assert len(response.json()['result']) == 0

def test_string_column_in_list():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species in ['human','mouse']"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_in_list_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species in ['HuMaN','mOuSe']"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_not_in_list():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species not in ['human']"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_not_in_list_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species not in ['HuMaN']"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_string_column_is_null():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species is null"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result'])  > 1

def test_string_column_is_not_null():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species is not null"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result'])  > 1



##### Incorrect use (Expected to fail)
def test_string_column_less_than_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species < 10"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "InvalidFilterError"

def test_string_column_less_than_or_equal_to_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species <= 10"]},
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "InvalidFilterError"

def test_string_column_greater_than_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species > 10"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "InvalidFilterError"

def test_string_column_greater_than_or_equal_to_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species >= 10"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "InvalidFilterError"

def test_string_column_equal_to_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = 10"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()["error_type"] == "InvalidFilterError"

def test_string_column_is_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species is true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"


def test_string_column_is_not_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species is not false"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

# TODO Bring up with team if we want false to be a valid string and not always default to a boolean
def test_string_column_equals_boolean_expected_failure(): 
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = false"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_string_column_equals_number_expected_failure(): 
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["species = 10"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"






################################ Boolean Column Operator testing ################################

##### Correct use
def test_boolean_column_equal_to_boolean():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc = true"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_boolean_column_equal_to_boolean_case_insensitivity():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc = tRuE"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_boolean_column_not_equal_to_boolean():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc != true"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_boolean_column_is_boolean():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc is true"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

def test_boolean_column_is_not_boolean():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc is true"]},
        params={'limit':10}
    )
    assert response.status_code == 200
    assert len(response.json()['result']) > 1

# TODO determine if we want this functionality, if so we will need to enforce case sensistivity or check first and convert to [True, False]
# def test_boolean_column_in_list_of_booleans():
#     response = client.post(
#         "/data/subject",
#         json={"MATCH_ALL": ["subject_data_at_gdc in [true]"]},
#         params={'limit':10}
#     )
#     assert response.status_code == 200
#     assert len(response.json()['result']) > 1




##### Incorrect use (Expected to fail)
def test_boolean_column_less_than_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc < true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_boolean_column_like_boolean_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc like true"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_boolean_column_equal_to_number_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc = 1"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"

def test_boolean_column_equal_to_string_expected_failure():
    response = client.post(
        "/data/subject",
        json={"MATCH_ALL": ["subject_data_at_gdc = STRING"]},
        params={'limit':10}
    )
    assert response.status_code == 400
    assert response.json()['error_type'] == "InvalidFilterError"















