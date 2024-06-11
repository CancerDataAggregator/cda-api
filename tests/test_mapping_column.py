from cda_api.db.query_utilities import get_mapping_column, get_mapping_columnname
from cda_api import MappingError

# Ensure collection name is set correctly
def test_get_mapping_columnname():
    entity_tablename = 'observation'
    foreign_tablename = 'subject'
    expected = 'subject_observation_alias_collection'
    actual = get_mapping_columnname(foreign_tablename=foreign_tablename, entity_tablename=entity_tablename)
    assert actual == expected


def test_get_mapping_column_success():
    entity_tablename = 'observation'
    foreign_tablename = 'subject'
    try:
        get_mapping_column(foreign_tablename=foreign_tablename, entity_tablename=entity_tablename)
    except:
        raise Exception
    

def test_get_mapping_column_table_dne():
    entity_tablename = 'observation'
    foreign_tablename = 'FAKE_TABLE'
    try:
        get_mapping_column(foreign_tablename=foreign_tablename, entity_tablename=entity_tablename)
    except MappingError as e:
        assert True
    except Exception as e:
        raise

