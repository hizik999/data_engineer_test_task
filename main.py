### импорт библиотек для нормальной работы с sqlite
from sqlalchemy import create_engine, inspect, Column, Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base


### создание движка и определение класса-родителя Base для всех моделей
engine = create_engine('sqlite:///mydb.db')
Base = declarative_base()

### модель магазина Store 
class Store(Base):
    __tablename__ = 'stores'

    id = Column(Integer, primary_key=True)
    city = Column(String(80))
    state = Column(String(80))

### модель покупателя Customer
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(Integer, primary_key=True)
    name = Column(String(80))
    signup_date = Column(String(80))
    ### обращаюсь к модели Store через __tablename__, чтобы лишний раз не повторяться и в случае изменения названия таблицы не пришлось бы менять названия и тут
    store_id = Column(Integer, ForeignKey(f'{Store.__tablename__}.id'))

    store = relationship(Store)

### модель покупки Transaction
class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True)
    ### обращаюсь к моделям Store и Customer через __tablename__, чтобы лишний раз не повторяться
    customer_id = Column(Integer, ForeignKey(f'{Customer.__tablename__}.id'))
    store_id = Column(Integer, ForeignKey(f'{Store.__tablename__}.id'))
    transaction_date = Column(String(80))
    category = Column(String(80))
    ### Decimal, потому что сумма покупки может быть нецелым числом
    amount = Column(Numeric(10, 2))

    ### связи с моделями Customer и Store для более правильной работы ORM
    customer = relationship(Customer)
    store = relationship(Store)
    

### создаем все таблицы
Base.metadata.create_all(engine)

### получаем список всех таблиц
inspector = inspect(engine)
tables = inspector.get_table_names()

### проверяем, что все таблицы создались
assert 'stores' in tables, "Таблица stores не создалась"
assert 'customers' in tables, "Таблица customers не создалась"
assert 'transactions' in tables, "Таблица transactions не создалась"

