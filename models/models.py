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
        # TODO: add [create or replace] instead of [create if not exists]

    )
    uri = Column(String, nullable=False)
    hash = Column(BigInteger, primary_key=True, )
    file = Column(String, nullable=False)
    category = Column(CategoryEnum, default=Category.OTHER)  # TODO: dim table for meta data about the categories/types
    date = Column(Integer, )
    downloaded = Column(Boolean, nullable=False)
    path = Column(String, nullable=True)

    add_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=func.now(), )


class YellowTaxi(BASE):  # class, model
    __tablename__ = 'yellow_taxi'
    __table_args__ = (
        PrimaryKeyConstraint('hash'),
        Index(
            'idx_yellow_taxi',
            'vendor_id', 'pep_pick_up_datetime', 'pep_drop_off_datetime', 'rate_code_id', 'pu_location_id',
            'passenger_count', 'trip_distance', 'do_location_id', 'payment_type', 'fare_amount', 'extra',
            'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge',
            'airport_fee', 'path', 'add_at',
        ),  # fasten the retrival queries
        Index('idx_dim_scrapper_pep_pick_up_datetime', 'pep_pick_up_datetime', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_pep_drop_off_datetime', 'pep_drop_off_datetime', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_rate_code_id', 'rate_code_id', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_pu_location_id', 'pu_location_id', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_do_location_id', 'do_location_id', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_passenger_count', 'passenger_count', ),  # fasten the retrival queries
        Index('idx_dim_scrapper_trip_distance', 'trip_distance'),  # fasten the retrival queries
        Index('idx_dim_scrapper_payment_type', 'payment_type'),  # fasten the retrival queries
        Index('idx_dim_scrapper_fare_amount', 'fare_amount'),  # fasten the retrival queries
        Index('idx_dim_scrapper_extra', 'extra'),  # fasten the retrival queries
        Index('idx_dim_scrapper_mta_tax', 'mta_tax'),  # fasten the retrival queries
        Index('idx_dim_scrapper_tip_amount', 'tip_amount'),  # fasten the retrival queries
        Index('idx_dim_scrapper_tolls_amount', 'tolls_amount'),  # fasten the retrival queries
        Index('idx_dim_scrapper_improvement_surcharge', 'improvement_surcharge'),  # fasten the retrival queries
        Index('idx_dim_scrapper_total_amount', 'total_amount'),  # fasten the retrival queries
        Index('idx_dim_scrapper_congestion_surcharge', 'congestion_surcharge'),  # fasten the retrival queries
        Index('idx_dim_scrapper_airport_fee', 'airport_fee'),  # fasten the retrival queries
        Index('idx_dim_scrapper_path', 'path'),  # fasten the retrival queries
        Index('idx_dim_scrapper_add_at', 'add_at'),  # fasten the retrival queries
        {'extend_existing': True, 'schema': SCHEMA, },
        # TODO: add [create or replace] instead of [create if not exists]

    )
    # TODO: know your data -- Data Model
    vendor_id = Column(Integer, nullable=False)
    pep_pick_up_datetime = Column(DateTime, nullable=True)
    pep_drop_off_datetime = Column(DateTime, nullable=True)
    passenger_count = Column(Float, nullable=True)
    trip_distance = Column(Float, nullable=True)
    rate_code_id = Column(Float, nullable=True)
    store_and_fwd_flag = Column(String, nullable=True)
    pu_location_id = Column(Integer, nullable=True)
    do_location_id = Column(Integer, nullable=True)
    payment_type = Column(BigInteger, nullable=True)
    fare_amount = Column(Float, nullable=True)
    extra = Column(Float, nullable=True)
    mta_tax = Column(Float, nullable=True)
    tip_amount = Column(Float, nullable=True)
    tolls_amount = Column(Float, nullable=True)
    improvement_surcharge = Column(Float, nullable=True)
    total_amount = Column(Float, nullable=True)
    congestion_surcharge = Column(Float, nullable=True)
    airport_fee = Column(Float, nullable=True)

    path = Column(String, nullable=False)  # enrichments
    hash = Column(BigInteger, primary_key=True, )
    add_at = Column(DateTime, default=datetime.utcnow, nullable=False)

