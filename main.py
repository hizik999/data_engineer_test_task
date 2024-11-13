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

    def __repr__(self):
        return f"Store(store_id={self.store_id!r}, city={self.city!r}, state={self.state!r})\n"
### модель покупателя Customer
class Customer(Base):
    __tablename__ = 'customers'

    customer_id = Column(Integer, primary_key=True)
    name = Column(String(80))
    signup_date = Column(String(80))
    store_id = Column(Integer, ForeignKey('stores.store_id'))

    store = relationship(Store)

    def __repr__(self):
        return f"Customer(customer_id={self.customer_id!r}, name={self.name!r}, signup_date={self.signup_date!r}, store_id={self.store_id!r})\n"
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
    
    def __repr__(self):
        return f"Transaction(transaction_id={self.transaction_id!r}, customer_id={self.customer_id!r}, store_id={self.store_id!r}, transaction_date={self.transaction_date!r}, category={self.category!r}, amount={self.amount!r})\n"

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

### Очистка таблицы перед загрузкой новых данных
session.query(Customer).delete()
session.query(Store).delete()
session.query(Transaction).delete()
session.commit()

### Загрузка данных через to_sql
customers_data.to_sql(Customer.__tablename__, engine, index=False, if_exists='append')
stores_data.to_sql(Store.__tablename__, engine, index=False, if_exists='append')
transactions_data.to_sql(Transaction.__tablename__, engine, index=False, if_exists='append')


### Проверка загрузки данных
customer = session.query(Customer).all()
store = session.query(Store).all()
transaction = session.query(Transaction).all()

# print(customer)
# print(store)
# print(transaction)


### селект запросы для заданий из части SQL (там все данные за 2023 год но я на всякий случай добавил фильтры по дате)
task1_query = """
                    with cte as (
                        SELECT 
                            t.store_id, 
                            count(*) as transactions_count
                        FROM 
                            transactions t 
                        WHERE t.transaction_date between '2023-01-01' and '2023-12-31'
                        GROUP BY 
                            t.store_id
                    )

                    SELECT 
                        s.store_id, 
                        s.city, 
                        s.state, 
                        cte.transactions_count
                    FROM 
                        stores s 
                    LEFT JOIN 
                        cte 
                    ON 
                        s.store_id = cte.store_id 
                    ORDER BY 
                        transactions_count DESC

"""

task2_query = """    
                    with cte as (
                        SELECT
                            t.customer_id,
                            count(*) as transactions_count
                        FROM 
                            transactions t
                        WHERE t.customer_id in (
                            SELECT 
                                c.customer_id
                            FROM
                            transactions t
                            LEFT JOIN 
                                customers c 
                            ON 
                                t.customer_id = c.customer_id 
                            WHERE 
                                julianday(t.transaction_date) - julianday(c.signup_date) < 31
                                AND 
                                c.signup_date between '2023-01-01' and '2023-12-31'
                        )
                        GROUP BY 
                            t.customer_id
                    )

                    SELECT 
                        c.customer_id, 
                        c.name, 
                        cte.transactions_count
                    FROM 
                        cte
                    LEFT JOIN 
                        customers c 
                    ON 
                        c.customer_id = cte.customer_id
"""



task3_query = """
                    SELECT 
                        t.category, 
                        sum(t.amount) as total_sales 
                    FROM 
                        transactions t 
                    WHERE 
                        t.transaction_date between '2023-01-01' and '2023-12-31'
                    GROUP BY 
                        t.category
                    ORDER BY 
                        total_sales DESC
                    LIMIT 3
"""



result1 = pd.read_sql_query(task1_query, engine)
result2 = pd.read_sql_query(task2_query, engine)
result3 = pd.read_sql_query(task3_query, engine)

result1.to_csv('outputs/store_transactions_2023.csv', index=False)
result2.to_csv('outputs/new_customers.csv', index=False)
result3.to_csv('outputs/top_categories.csv', index=False)

session.close()