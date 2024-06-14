from dotenv import load_dotenv, find_dotenv
from os import getenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Function to determine if the database is being accessed from an interactive python scriot or not 
# (Changes the pathing for the .env file)
def is_interactive():
    import __main__ as main
    return not hasattr(main, '__file__')

# Load environment variables
if is_interactive():
    env_file = find_dotenv('cda_api/config/.env')
else:
    env_file = find_dotenv('../config/.env')
load_dotenv(env_file)

# Use environment variables to establish database connection url
DB_USERNAME = getenv('DB_USERNAME')
DB_PASSWORD = getenv('DB_PASSWORD')
DB_HOSTNAME = getenv('DB_HOSTNAME')
DB_PORT = getenv('DB_PORT')
DB_DATABASE = getenv('DB_DATABASE')
SQLALCHEMY_DATABASE_URL = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOSTNAME}:{DB_PORT}/{DB_DATABASE}'

# Create sqlalchemy database engine object and Session
engine = create_engine(SQLALCHEMY_DATABASE_URL)
# TODO determine if there is a better (more secure) way to set up sessions
session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
