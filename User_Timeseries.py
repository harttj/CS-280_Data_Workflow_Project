from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, Date

Base = declarative_base()

class User_Timeseries(Base):
    __tablename__ = "user_timeseries"
    id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(BigInteger, nullable=False)
    followers_count = Column(Integer, nullable=False)
    following_count = Column(Integer, nullable=False)
    tweet_count = Column(Integer, nullable=False)
    listed_count = Column(Integer, nullable=False)
    date = Column(Date,nullable=False)

    
    def __repr__(self) -> str:
        return f"User_Timeseries(id={self.id}, user_id={self.user_id}, followers_count={self.followers_count}, following_count={self.following_count}, tweet_count={self.tweet_count}, listed_count={self.listed_count}, date={self.date})" 