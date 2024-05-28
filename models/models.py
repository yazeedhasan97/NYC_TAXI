from sqlalchemy.sql import func
from datetime import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Enum, Float, Boolean, ARRAY, PrimaryKeyConstraint, BigInteger, \
    Index

from models.consts import Category
from models.utils import Model

BASE = declarative_base(cls=Model)  # DDL
SCHEMA = 'nyc_taxi_analysis'  # case sensitive



# ORM: object Relational Mapper: bridge to connect OOP programs [models] to DB [tables]

CategoryEnum = Enum(Category, name='CategoryEnum', schema=SCHEMA)


class URIs(BASE):  # class, model
    __tablename__ = 'dim_scrapper'
    __table_args__ = (
        PrimaryKeyConstraint('uri'),
        Index('idx_dim_scrapper', 'uri', 'file', 'category', 'date', 'downloaded'),  # fasten the retrival queries
        Index('idx_dim_scrapper_uri', 'uri', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_file', 'file', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_category', 'category', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_date', 'date', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_downloaded', 'downloaded'),  # fasten the retrival queries
        {'extend_existing': True, 'schema': SCHEMA, },

    )
    uri = Column(String, nullable=False)
    hash = Column(BigInteger, primary_key=True, )
    file = Column(String, nullable=False)
    category = Column(CategoryEnum, default=Category.OTHER)
    date = Column(Integer, )
    downloaded = Column(Boolean, nullable=False)

    add_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=func.now(),)


    # class Bike(BASE):  # class, model
    #     __tablename__ = 'bikes'
    #     __table_args__ = (
    #         PrimaryKeyConstraint('id'),
    #         {'extend_existing': True, 'schema': SCHEMA, },
    #
    #     )
    #
    #     # Note: models doesn't provide validation for the data [by default], error will happen on the database level
    #     # -- we must provide validation - through encapsulation
    #     id = Column(Integer, autoincrement=True, primary_key=True, )
    #
    #
    # class Audit(BASE):  # class, model # track the performance of your application
    #     # audits - aims for external[business] use and to track of dashboards
    #
    #     __tablename__ = 'audits'
    #     __table_args__ = (
    #         PrimaryKeyConstraint('id'),
    #         {'extend_existing': True, 'schema': SCHEMA, },
    #
    #     )
    #
    #     # user = ....
    #     # os = ...
    #     # location = ...
    #     id = Column(Integer, autoincrement=True, primary_key=True, )
    #     os = Column(String, nullable=False, )
    #     ip = Column(String, nullable=False, )
    #     created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    #     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=func.now(), nullable=False)
    #
    #     pass

    # class DataQualityInput(BASE):
    # __tablename__ = 'data_quality_inputs'
    #
    # owner = Column(String, nullable=True, )
    # schema = Column(String, nullable=False, default='feeds')
    # feed = Column(String, nullable=False)  # the only required field
    # type = Column(Enum(FeedType), default=FeedType.DUMP, nullable=False)
    # engine = Column(Enum(Engine), default=Engine.KAMANJA, nullable=False)
    # # best_run_time = Column(Enum(FeedType), default=FeedType.DUMP, nullable=False)
    #
    # threshold = Column(Float, default=0.1, nullable=False)
    # window = Column(Integer, default=7, nullable=False)
    # partitioned_by = Column(String, default='tbl_dt', nullable=False)
    # regex = Column(String, default='([0-9]{8}).*$', )
    # ignore_files_with_regex = Column(String, default='%.tar.gz$', )
    # seasonality = Column(Enum(Frequency), default=Frequency.WEEKLY, nullable=False)
    #
    # last_available_stats_day = Column(Integer, nullable=True)
    # frequency = Column(Enum(Frequency), default=Frequency.DAILY, nullable=False)
    # msck = Column(Boolean, default=True, nullable=False)
    # dedup = Column(Boolean, default=True, nullable=False)
    # pcf = Column(Boolean, default=True, nullable=False)
    # trend = Column(Boolean, default=True, nullable=False)
    # match = Column(Boolean, default=True, nullable=False)
    #
    # # if true meaning it was not found in file stats but still registered in the system
    # decommissioned = Column(Boolean, default=False, nullable=False)
    # decommission_after_days = Column(Integer, default=92, nullable=False)
    # created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # updated_at = Column(DateTime, default=datetime.utcnow, onupdate=func.now(), nullable=False)
    # removed_at = Column(DateTime, default=None, nullable=True)
    #
    # __table_args__ = (
    #     PrimaryKeyConstraint('schema', 'feed'),
    #     {'extend_existing': True, 'schema': DQ_SCHEMA, },
    #
    # )
    # pass
