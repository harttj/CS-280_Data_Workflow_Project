from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Numeric, Integer, Text, Date

Base = declarative_base()

class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(Integer, primary_key=True, nullable=False)
    tweet_id = Column(Numeric, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    text = Column(Text, nullable=False)
    created_at = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return f"Tweet(id={self.id}, tweet_id={self.tweet_id}, user_id={self.user_id}, text={self.text}, date={self.created_at}" 