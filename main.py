from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
import pandas as pd
### иморт движка бд
from database import engine, create_tables
### иморт моделей Store, Customer, Transaction
from models import *
### sql запросы из категории "SQL часть"
from sql_queries import *

### создаем все таблицы
create_tables()

### получаем список всех таблиц
inspector = inspect(engine)
tables = inspector.get_table_names()

### проверяем, что все таблицы создались
assert Store.__tablename__ in tables, f"Таблица {Store.__tablename__} не создалась"
assert Customer.__tablename__ in tables, f"Таблица {Customer.__tablename__} не создалась"
assert Transaction.__tablename__ in tables, f"Таблица {Transaction.__tablename__} не создалась"
print('Таблицы созданы')

### забираем данные из Excel файлов с помощью read_excel
customers_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='customers')
stores_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='stores')
transactions_data = pd.read_excel('./data/tobacco_company_data.xlsx', sheet_name='transactions')
print("Данные получены")

### создаем сессию для работы с базой данных
Session = sessionmaker(bind=engine)
session = Session()

### Очистка таблицы перед загрузкой данных (для перезапуска кода несколько раз)
session.query(Store).delete()
session.query(Customer).delete()
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
print("Данные загружены")


### выполнение запросов и запись их в датафреймы
result1 = pd.read_sql_query(task1_query, engine)
result2 = pd.read_sql_query(task2_query, engine)
result3 = pd.read_sql_query(task3_query, engine)
print("Запросы выполнены")

### запись датафреймов результатов в csv в папку outputs/
result1.to_csv('outputs/store_transactions_2023.csv', index=False)
result2.to_csv('outputs/new_customers.csv', index=False)
result3.to_csv('outputs/top_categories.csv', index=False)
print("Результаты записаны")

### предобработка транзакций для анализа роста продаж
transactions_data['transaction_date'] = transactions_data['transaction_date'].astype('datetime64[ns]')
transactions_data['month'] = transactions_data['transaction_date'].dt.month
transactions_data = transactions_data.where(lambda x: x['transaction_date'].dt.year == 2023).groupby('month')['amount'].sum().reset_index()
#print(transactions_data)
print("Транзакции предобработаны")

### функция анализа процентного роста продаж
def analyse_sale_growth(df: pd.DataFrame):
    df['growth_rate'] = df['amount'].pct_change() * 100
    df['growth_rate'] = df['growth_rate'].round(1)
    df = df.drop('amount', axis=1)
    return df

print("Рассчет процентого роста продаж...")
print(analyse_sale_growth(transactions_data))
print("Процентный рост по месяцам посчитан")

session.close()
print("Задания выполнены")