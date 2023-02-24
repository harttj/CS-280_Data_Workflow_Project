from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Numeric, Integer, Date

Base = declarative_base()

class Tweet_Timeseries(Base):
    __tablename__ = "tweet_timeseries"
    id = Column(Integer, primary_key=True, nullable=False)
    tweet_id = Column(Numeric, nullable=False)
    retweet_count = Column(Integer, nullable=True)
    favorite_count = Column(Integer, nullable=True)
    date = Column(Date, nullable=True)

    
    def __repr__(self) -> str:
        return f"Tweet_Timeseries(id={self.id}, tweet_id={self.tweet_id}, retweet_count={self.retweet_count}, favorite_count={self.favorite_count}, date={self.date})" 