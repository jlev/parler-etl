from sqlalchemy import Column, Integer, String, JSON, Boolean, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

base = declarative_base()

class Post(base):
    __tablename__ = 'posts'

    id = Column(String, primary_key=True)
    creator = Column(String)
    created_at = Column(DateTime)
    body = Column(String)
    impression_count = Column(Integer)
    comment_count = Column(Integer)
    upvote_count = Column(Integer)
    links = Column(JSON)

class User(base):
    __tablename__ = 'users'

    id = Column(String, primary_key=True)
    username = Column(String)
    banned = Column(Boolean)
    bio = Column(String)
    profile_photo = Column(String)
    followers = Column(Integer)
    following = Column(Integer)
    posts = Column(Integer)
    joined = Column(DateTime)
    verified = Column(Boolean)

class Metadata(base):
    __tablename__ = 'metadata'

    id = Column(String, primary_key=True)
    created_at = Column(DateTime)
    lat = Column(Float)
    lon = Column(Float)
    exif = Column(JSON)
