## This is for running in debugger mode

from fastapi.testclient import TestClient
from cda_api import app

client = TestClient(app)

# Change the endpoint
ENDPOINT = "/data/subject"

# OPTIONAL set columnname for unique_values endpoint
COLUMNNAME = 'sex'

# OPTIONAL: Change the QNODE variable to the body you'd like to test
QNODE = {"MATCH_ALL": ["subject_id_alias < 0"]}

def test_debug_unique_values():
    # Set arguements
    args = {
          'columnname': 'sex',      # str
          'system': None,           # str
          'count': False,           # True | False
          'totalCount': False,      # True | False
          'limit': None,            # None | int
          'offset': None,           # None | int
    }
    
    arg_strings = [f"{key}={str(value).lower()}" for key, value in args.items() if (key != 'columnname') and (value != None)]
    endpoint = f'unique_values/{args["columnname"]}?' + '&'.join(arg_strings)
    
    response = client.post(
            endpoint
            )
    assert True

def test_debug_data_or_summary():
    response = client.post(
                ENDPOINT,
                json=QNODE,
                )
    assert True