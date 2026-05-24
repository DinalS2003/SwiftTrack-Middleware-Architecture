"""
Database Models for Order Service
Using SQLAlchemy ORM for persistent storage
"""

from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://swiftlogistics:swiftlogistics_pass@localhost:5432/swiftlogistics_db'
)

if DATABASE_URL.startswith('sqlite'):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=10, max_overflow=20)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Order(Base):
    """Order Model"""
    __tablename__ = "orders"

    order_id = Column(String, primary_key=True, index=True)
    client_id = Column(String, index=True)
    recipient_name = Column(String)
    phone = Column(String)
    delivery_address = Column(String)
    address = Column(String)
    city = Column(String)
    zip = Column(String)
    status = Column(String, default='processing')
    priority = Column(String, default='normal')
    notes = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    processing_at = Column(DateTime, default=datetime.utcnow)
    confirmed_at = Column(DateTime, nullable=True)
    dispatched_at = Column(DateTime, nullable=True)
    collected_at = Column(DateTime, nullable=True)
    delivered_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=True)
    estimated_delivery = Column(DateTime, nullable=True)
    driver_id = Column(String, nullable=True)
    driver_name = Column(String, nullable=True)
    current_location = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'order_id': self.order_id,
            'client_id': self.client_id,
            'recipient_name': self.recipient_name,
            'phone': self.phone,
            'delivery_address': self.delivery_address,
            'address': self.address,
            'city': self.city,
            'zip': self.zip,
            'status': self.status,
            'priority': self.priority,
            'notes': self.notes,
            'created_at': self.created_at.isoformat(),
            'processing_at': self.processing_at.isoformat() if self.processing_at else None,
            'confirmed_at': self.confirmed_at.isoformat() if self.confirmed_at else None,
            'dispatched_at': self.dispatched_at.isoformat() if self.dispatched_at else None,
            'collected_at': self.collected_at.isoformat() if self.collected_at else None,
            'delivered_at': self.delivered_at.isoformat() if self.delivered_at else None,
            'failed_at': self.failed_at.isoformat() if self.failed_at else None,
            'estimated_delivery': self.estimated_delivery.isoformat() if self.estimated_delivery else None,
            'driver_id': self.driver_id,
            'driver_name': self.driver_name,
            'current_location': self.current_location,
            'packages': 0,
        }


class Package(Base):
    """Package Model"""
    __tablename__ = "packages"

    package_id = Column(String, primary_key=True, index=True)
    order_id = Column(String, index=True)
    weight = Column(Float, default=2.0)
    dimensions = Column(String, default='10x10x10cm')
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'package_id': self.package_id,
            'order_id': self.order_id,
            'weight': self.weight,
            'dimensions': self.dimensions,
        }


class Delivery(Base):
    """Delivery Model"""
    __tablename__ = "deliveries"

    delivery_id = Column(String, primary_key=True, index=True)
    order_id = Column(String, index=True)
    client_id = Column(String, index=True)
    status = Column(String, default='pending')  # pending, delivered, failed
    driver_id = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    proof_type = Column(String, nullable=True)  # signature, photo
    proof_data = Column(Text, nullable=True)  # base64 encoded
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'delivery_id': self.delivery_id,
            'order_id': self.order_id,
            'client_id': self.client_id,
            'status': self.status,
            'driver_id': self.driver_id,
            'notes': self.notes,
            'updated_at': self.updated_at.isoformat(),
        }


class SagaState(Base):
    """Saga Orchestration State Tracking"""
    __tablename__ = "saga_states"

    order_id = Column(String, primary_key=True, index=True)
    status = Column(String, default='pending')  # pending, in-progress, completed, compensated
    steps_completed = Column(Text)  # JSON array as string
    compensation_needed = Column(Boolean, default=False)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'order_id': self.order_id,
            'status': self.status,
            'steps_completed': self.steps_completed,
            'compensation_needed': self.compensation_needed,
            'error': self.error,
        }


class PublishedEvent(Base):
    """Track Published Events to RabbitMQ"""
    __tablename__ = "published_events"

    event_id = Column(String, primary_key=True, index=True)
    event_type = Column(String, index=True)
    order_id = Column(String, index=True, nullable=True)
    delivery_id = Column(String, index=True, nullable=True)
    data = Column(Text)  # JSON as string
    published_at = Column(DateTime, default=datetime.utcnow, index=True)

    def to_dict(self):
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'order_id': self.order_id,
            'delivery_id': self.delivery_id,
            'data': self.data,
            'published_at': self.published_at.isoformat(),
        }


def init_db():
    """Initialize database and create tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Database initialization error: {str(e)}")


def get_db():
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
