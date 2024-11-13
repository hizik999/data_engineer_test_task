from sqlalchemy import create_engine, inspect, Column, Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import pandas as pd

### создание движка и определение класса-родителя Base для всех моделей
engine = create_engine('sqlite:///mydb.db')
Base = declarative_base()

### модель магазина Store 
class Store(Base):
    __tablename__ = 'stores'

    store_id = Column(Integer, primary_key=True)
    city = Column(String(80))
    state = Column(String(80))

### модель покупателя Customer
class Customer(Base):
    __tablename__ = 'customers'

    customer_id = Column(Integer, primary_key=True)
    name = Column(String(80))
    signup_date = Column(String(80))
    store_id = Column(Integer, ForeignKey('stores.store_id'))

    store = relationship(Store)

### модель покупки Transaction
class Transaction(Base):
    __tablename__ = 'transactions'

    transaction_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    store_id = Column(Integer, ForeignKey('stores.store_id'))
    transaction_date = Column(String(80))
    category = Column(String(80))
    amount = Column(Numeric(10, 2))

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

### забираем данные из Excel файлов с помощью read_excel
customers_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='customers')
stores_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='stores')
transactions_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='transactions')

### создаем сессию для работы с базой данных
Session = sessionmaker(bind=engine)
session = Session()

# Очистка таблицы перед загрузкой новых данных
session.query(Customer).delete()
session.query(Store).delete()
session.query(Transaction).delete()
session.commit()

# Загрузка данных через to_sql
customers_data.to_sql(Customer.__tablename__, engine, index=False, if_exists='append')
stores_data.to_sql(Store.__tablename__, engine, index=False, if_exists='append')
transactions_data.to_sql(Transaction.__tablename__, engine, index=False, if_exists='append')


# Проверка данных
customer = session.query(Customer).all()
store = session.query(Store).all()
transaction = session.query(Transaction).all()

print(customer)
print(store)
print(transaction)
