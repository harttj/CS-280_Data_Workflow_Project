from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, VARCHAR, Date

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    username = Column(VARCHAR, nullable=False)
    name = Column(VARCHAR, nullable=False)
    created_at = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return f"User(id={self.id}, user_id={self.user_id}, username={self.username}, name={self.name}, created_at={self.created_at})" 