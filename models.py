from sqlalchemy import Column, Integer, String, JSON, Boolean, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

base = declarative_base()

class Post(base):
    __tablename__ = 'posts'

    id = Column(String, primary_key=True)
    author_username = Column(String)
    author_name = Column(String)
    author_profile_img_url = Column(String)
    title = Column(String)
    created_at = Column(String)
    approx_created_at = Column(DateTime)
    body = Column(String)
    impression_count = Column(Integer)
    comment_count = Column(Integer)
    echo_count = Column(Integer)
    upvote_count = Column(Integer)
    is_echo = Column(Boolean)
    echo = Column(JSON)
    media = Column(JSON)

class Metadata(base):
    __tablename__ = 'metadata'

    id = Column(String, primary_key=True)
    created_at = Column(DateTime)
    lat = Column(Float)
    lon = Column(Float)