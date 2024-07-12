from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy import Column, Table
from dataclasses import dataclass, field

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
    
    def __post_init__(self): 
        self.columnname = self.metadata_column.name
        self.tablename = self.metadata_table.name
        self.table_columnname = f'{self.tablename}.{self.columnname}'
        if self.entity_table:
            self.entity_column = getattr(self.entity_table, self.columnname)
        else:
            self.entity_column = None

    def in_entity_table(self):
        return bool(self.entity_table)
