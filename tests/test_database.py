from cda_api.db.connection import engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_engine_connection():
    try:
        db_session = TestingSessionLocal()
        db_session.execute(text("SELECT 1"))
        db_session.close()
    except:
        raise Exception  # TODO better exception
