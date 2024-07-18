from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import Column, Table
from dataclasses import dataclass, field


temp_column_category_map = {
    'file': {
        'type': 'categorical', 
        'format': 'categorical', 
        'category': 'categorical', 
        'size': 'continuous'
    },
    'subject': {
        'species': 'categorical',
        'year_of_birth': 'continuous',
        'year_of_death': 'continuous',
        'cause_of_death': 'categorical',
        'race': 'categorical',
        'ethnicity': 'categorical'
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
            if self.tablename in temp_column_category_map.keys():
                if self.columnname in temp_column_category_map[self.tablename].keys():
                    self.category = temp_column_category_map[self.tablename][self.columnname]
        else:
            self.entity_column = None

    def in_entity_table(self):
        return bool(self.entity_table)
