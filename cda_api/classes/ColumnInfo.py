from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import Column, Table
from dataclasses import dataclass, field
# from cda_api.db import COLUMN_TYPE_MAP

COLUMN_TYPE_MAP = {
    'file': {
        'access': 'categorical',
        'size': 'numeric',
        'checksum_type': 'categorical',
        'format': 'categorical',
        'type': 'categorical',
        'category': 'categorical',
        'anatomic_site': 'categorical',
        'tumor_vs_normal': 'categorical'
        },
    'observation': {
        'vital_status': 'categorical',
        'sex': 'categorical',
        'year_of_observation': 'numeric',
        'diagnosis': 'categorical',
        'morphology': 'categorical',
        'grade': 'categorical',
        'stage': 'categorical',
        'observed_anatomic_site': 'categorical',
        'resection_anatomic_site': 'categorical'
    },
    'project': {'type': 'categorical'},
    'subject': {
        'species': 'categorical',
        'year_of_birth': 'numeric',
        'year_of_death': 'numeric',
        'cause_of_death': 'categorical',
        'race': 'categorical',
        'ethnicity': 'categorical'
        },
    'treatment': {
        'anatomic_site': 'categorical',
        'type': 'categorical',
        'therapeutic_agent': 'categorical'
        }
  }

@dataclass
class ColumnInfo():
    unqiuename: str
    entity_table: DeclarativeMeta
    metadata_table: Table
    metadata_column: Column
    columnname: str = field(init=False)
    tablename: str = field(init=False)
    table_columnname: str = field(init=False)
    entity_column: InstrumentedAttribute = field(init=False)
    category: str = field(init=False)
    
    def __post_init__(self): 
        self.columnname = self.metadata_column.name
        self.tablename = self.metadata_table.name
        self.table_columnname = f'{self.tablename}.{self.columnname}'
        self.category = None
        if self.entity_table:
            self.entity_column = getattr(self.entity_table, self.columnname)
            if self.tablename in COLUMN_TYPE_MAP.keys():
                if self.columnname in COLUMN_TYPE_MAP[self.tablename].keys():
                    self.category = COLUMN_TYPE_MAP[self.tablename][self.columnname]
        else:
            self.entity_column = None

    def in_entity_table(self):
        return bool(self.entity_table)

