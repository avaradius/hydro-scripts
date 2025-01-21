import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

class DatabaseConnection:
    def __init__(self):
        os.environ.pop("DB_HOST", None)
        os.environ.pop("DB_PORT", None)
        os.environ.pop("DB_NAME", None)
        os.environ.pop("DB_USER", None)
        os.environ.pop("DB_PASSWORD", None)
        load_dotenv()

        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')

        self.engine = None
        self.Session = None

        self.connect()

    def connect(self):
        try:
            connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(connection_string, echo=True)
            self.Session = sessionmaker(bind=self.engine)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Connection successful")
        except SQLAlchemyError as e:
            print(f"Error connecting to database: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
