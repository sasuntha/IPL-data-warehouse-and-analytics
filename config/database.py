
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:

    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'ipl_analytics')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '26557')
        
    @property
    def connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_engine(self):
        return create_engine(
            self.connection_string,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20
        )
    
    def get_session(self):
        engine = self.get_engine()
        Session = sessionmaker(bind=engine)
        return Session()

db_config = DatabaseConfig()