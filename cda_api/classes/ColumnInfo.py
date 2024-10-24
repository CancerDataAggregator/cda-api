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
    column_metadata_map: dict
    columnname: str = field(init=False)
    tablename: str = field(init=False)
    table_columnname: str = field(init=False)
    entity_column: InstrumentedAttribute = field(init=False)
    column_type: str = field(init=False)
    summary_display: bool = field(init=False)
    fetch_rows_returns: bool = field(init=False)
    process_before_display: str = field(init=False)
    virtual_table: str = field(init=False)

    def __post_init__(self): 
        self.columnname = self.metadata_column.name
        self.tablename = self.metadata_table.name
        self.table_columnname = f'{self.tablename}.{self.columnname}'

        # Set metadata
        self.column_type = None
        self.summary_display = None
        self.fetch_rows_returns = None
        self.process_before_display = None
        self.virtual_table = None

        if self.entity_table:
            self.entity_column = getattr(self.entity_table, self.columnname)
            if self.tablename in self.column_metadata_map.keys():
                if self.columnname in self.column_metadata_map[self.tablename].keys():
                    column_metadata = self.column_metadata_map[self.tablename][self.columnname]
                    self.column_type = column_metadata['column_type']
                    self.summary_display = column_metadata['summary_display']
                    self.fetch_rows_returns = column_metadata['fetch_rows_returns']
                    self.process_before_display = column_metadata['process_before_display']
                    self.virtual_table = column_metadata['virtual_table']

        else:
            self.entity_column = None

    def in_entity_table(self):
        return bool(self.entity_table)

