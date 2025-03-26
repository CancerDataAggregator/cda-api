from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import relationship, foreign
from sqlalchemy import and_

from cda_api import get_logger
from cda_api.db.connection import engine
from cda_api.db.query_operators import case_insensitive_equals

log = get_logger("Setup: schema.py")


# Ran into issue with self referential table relationship collection
# The following resolves the naming issue
def name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    disc = "_".join(col.name for col in constraint.columns)
    return referred_cls.__name__.lower() + "_" + disc + "_collection"


try:
    log.info("Building SQLAlchemy automap")
    Base = automap_base()
    Base.prepare(autoload_with=engine, name_for_collection_relationship=name_for_collection_relationship)
    TABLE_LIST = Base.classes.values()
    log.info("Successfully built SQLAlchemy automap")
except Exception as e:
    log.exception(e)
    raise e

try:
    # Map entitity tables with subject_alias and from file
    log.info("Adding relationships to file table")

    file_describes_subject = Base.metadata.tables["file_describes_subject"]

    Base.classes.file.observation = relationship(
        "observation",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        secondaryjoin=Base.classes.observation.subject_alias == file_describes_subject.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.file.mutation = relationship(
        "mutation",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        secondaryjoin=Base.classes.mutation.subject_alias == file_describes_subject.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.file.treatment = relationship(
        "treatment",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        secondaryjoin=Base.classes.treatment.subject_alias == file_describes_subject.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.observation.file = relationship(
        "file",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.observation.subject_alias == file_describes_subject.columns["subject_alias"],
        secondaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        viewonly=True,
    )

    Base.classes.mutation.file = relationship(
        "file",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.mutation.subject_alias == file_describes_subject.columns["subject_alias"],
        secondaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        viewonly=True,
    )

    Base.classes.treatment.file = relationship(
        "file",
        secondary=file_describes_subject,
        primaryjoin=Base.classes.treatment.subject_alias == file_describes_subject.columns["subject_alias"],
        secondaryjoin=Base.classes.file.id_alias == file_describes_subject.columns["file_alias"],
        viewonly=True,
    )

    # Map entitity tables with subject_alias and from project
    log.info("Adding relationships to project table")

    subject_in_project = Base.metadata.tables["subject_in_project"]

    Base.classes.project.observation = relationship(
        "observation",
        secondary=subject_in_project,
        primaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        secondaryjoin=Base.classes.observation.subject_alias == subject_in_project.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.project.mutation = relationship(
        "mutation",
        secondary=subject_in_project,
        primaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        secondaryjoin=Base.classes.mutation.subject_alias == subject_in_project.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.project.treatment = relationship(
        "treatment",
        secondary=subject_in_project,
        primaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        secondaryjoin=Base.classes.treatment.subject_alias == subject_in_project.columns["subject_alias"],
        viewonly=True,
    )

    Base.classes.observation.project = relationship(
        "project",
        secondary=subject_in_project,
        primaryjoin=Base.classes.observation.subject_alias == subject_in_project.columns["subject_alias"],
        secondaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        viewonly=True,
    )

    Base.classes.mutation.project = relationship(
        "project",
        secondary=subject_in_project,
        primaryjoin=Base.classes.mutation.subject_alias == subject_in_project.columns["subject_alias"],
        secondaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        viewonly=True,
    )

    Base.classes.treatment.project = relationship(
        "project",
        secondary=subject_in_project,
        primaryjoin=Base.classes.treatment.subject_alias == subject_in_project.columns["subject_alias"],
        secondaryjoin=Base.classes.project.id_alias == subject_in_project.columns["project_alias"],
        viewonly=True,
    )


    # # Mapping upstream_identifiers to file and subject
    # log.info("Adding relationships to upstream_identifiers table")

    # upstream_identifiers = Base.classes.upstream_identifiers

    # Base.classes.subject.upstream_identifiers = relationship(
    #     "upstream_identifiers",
    #     primaryjoin=and_(foreign(Base.classes.subject.id_alias) == upstream_identifiers.id_alias, 
    #                      case_insensitive_equals(upstream_identifiers.cda_table, 'subject')),
    #     viewonly=True,
    # )

    # Base.classes.file.upstream_identifiers = relationship(
    #     "upstream_identifiers",
    #     primaryjoin=and_(foreign(Base.classes.file.id_alias) == upstream_identifiers.id_alias, 
    #                      case_insensitive_equals(upstream_identifiers.cda_table, 'file')),
    #     viewonly=True,
    # )

except Exception as e:
    log.exception(e)
    raise e
# for tablename, table in Base.classes.items():
#     print(tablename, [r.target.name for r in inspect(table).relationships])
#     current_table_relationships = [r.target.name for r in inspect(table).relationships] + [tablename]
#     for target_tablename, target_table in Base.classes.items():
#         if target_tablename in current_table_relationships:
#             continue
#         meta_table = Base.metadata.tables[tablename]
#         meta_target_table = Base.metadata.tables[target_tablename]
#         if ('subject_alias' in meta_table.columns.keys()) and ('subject_alias' in meta_target_table.columns.keys()):
#             continue
#         elif ('subject_alias' in meta_table.columns.keys()):
#             try:
#                 r = [r for r in inspect(target_table).relationships if r.target.name == 'subject'][0]
#                 subject_mapping_table = r.secondary
#                 target_local_column, target_mapping_column = [(local, remote) for (local, remote) in r.local_remote_pairs if local.table.name == target_tablename][0]
#             except:
#                 raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
#             primary_join = table.subject_alias == subject_mapping_table.columns['subject_alias']
#             secondary_join = target_local_column == target_mapping_column
#             rel = relationship(target_tablename,
#                                primaryjoin = primary_join,
#                                secondaryjoin = secondary_join,
#                                secondary = subject_mapping_table,
#                                viewonly=True)
#         elif ('subject_alias' in meta_target_table.columns.keys()):
#             try:
#                 r = [r for r in inspect(table).relationships if r.target.name == 'subject'][0]
#                 subject_mapping_table = r.secondary
#                 local_column, mapping_column = [(local, remote) for (local, remote) in r.local_remote_pairs if local.table.name == tablename][0]
#             except:
#                 raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
#             primary_join = local_column == mapping_column
#             secondary_join = target_table.subject_alias == subject_mapping_table.columns['subject_alias']
#             rel = relationship(target_tablename,
#                                primaryjoin = primary_join,
#                                secondaryjoin = secondary_join,
#                                secondary = subject_mapping_table,
#                                viewonly=True)
#         else:
#             raise RelationshipError(f'Error mapping between {tablename} and {target_tablename}')
#         setattr(table, f'{target_tablename}', rel)
