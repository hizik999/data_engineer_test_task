from sqlalchemy import Column, Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import relationship
from database import Base


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
