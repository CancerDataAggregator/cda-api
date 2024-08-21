import cda_api.db
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import Column, Table
from dataclasses import dataclass, field

@dataclass
class ColumnInfo():
    uniquename: str
    entity_table: DeclarativeMeta
    metadata_table: Table
    metadata_column: Column
    column_map: dict
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
            if self.tablename in self.column_map.keys():
                if self.columnname in self.column_map[self.tablename].keys():
                    self.category = self.column_map[self.tablename][self.columnname]
        else:
            self.entity_column = None

    def in_entity_table(self):
        return bool(self.entity_table)

