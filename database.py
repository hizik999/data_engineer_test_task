from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

### создание движка и определение класса-родителя Base для всех моделей
engine = create_engine('sqlite:///mydb.db')
Base = declarative_base()

def create_tables():
    Base.metadata.create_all(engine)