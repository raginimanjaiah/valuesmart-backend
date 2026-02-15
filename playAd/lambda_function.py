import os
import json
import logging
import datetime
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, BigInteger, Enum, Date, DateTime, ForeignKey, text
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
from pydantic import BaseModel, Field, ValidationError
from datetime import datetime, timezone

Base = declarative_base()

class SlotBookingRequest(Base):
    __tablename__ = 'slot_booking_request'
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_id = Column(String(100), nullable=False, unique=True)
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(100))
    division_id = Column(Integer, ForeignKey('division.id'), nullable=False) # Assumes a 'division' table exists
    booking_date_id = Column(Integer, nullable=False) 
    # Booking Status Enum
    booking_status = Column(
        Enum('o', 'w', 'c', 'ex', name='booking_status_enum'), 
        nullable=False
    )  
    booking_points = Column(BigInteger, nullable=False)
    waiting_position = Column(Integer, default=None)
    url = Column(String(500))    
    # Approval Status Enum
    approval_status = Column(
        Enum('pending', 'approved', 'rejected', name='approval_status_enum'), 
        nullable=False, 
        server_default='pending'
    )   
    cancellation_date = Column(DateTime, server_default=func.current_timestamp())
    approval_status_date = Column(DateTime, server_default=func.current_timestamp())
    file_upload_date = Column(DateTime, server_default=func.current_timestamp())

    # Relationship (Optional: if you have a Division model)
    # division = relationship("Division", back_populates="bookings")


class CalendarTimeRates(Base):
    __tablename__ = 'calendar_time_rates'
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_date = Column(Date, nullable=False, comment='Calendar date available for booking')
    hour = Column(String(5), nullable=False, comment='Hour of the day in 24-hour format (0–23)')
    slot_number = Column(Integer, nullable=False, comment='Unique slot number for the hour/day')
    rate_card_id = Column(Integer, nullable=False, comment='Price for booking the slot in currency')
    
# --- 3. PYDANTIC SCHEMAS (Validation Layer) ---

class GetDivSchema(BaseModel):
    divisionId: int
    


# Configure Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Configuration & Global Initializations ---
# DB_USER = os.environ.get("DB_USER")
# DB_PASS = os.environ.get("DB_PASS")
# DB_HOST = os.environ.get("DB_HOST")
# DB_NAME = os.environ.get("DB_NAME")
# BUCKET_NAME = os.environ.get("BUCKET_NAME", "valuesmart-assets")




# Global engine and session factory to leverage Lambda warm starts
_engine = None
_SessionLocal = None
s3_client = boto3.client('s3')

def get_session():
    """Initializes the engine once and returns a new session."""
    global _engine, _SessionLocal
    if _SessionLocal is None:
        try:
            connection_uri = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
            _engine = create_engine(
                connection_uri,
                pool_recycle=300,  # AWS RDS often kills idle connections after 5 mins
                pool_pre_ping=True, # Validates connection before use
                connect_args={"connect_timeout": 5}
            )
            _SessionLocal = sessionmaker(bind=_engine)
        except Exception as e:
            logger.error(f"Database engine initialization failed: {str(e)}")
            raise
    return _SessionLocal()

def send_return_status(status_code, data):
    """Utility to format the API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(data)
    }

def lambda_handler(event, context):
    logger.info("Event received: %s", json.dumps(event))
    
    session = None
    try:
        session = get_session()
        return get_play_ad(event, session)
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        return send_return_status(503, {"error": "Service temporarily unavailable"})
    except Exception as e:
        logger.exception(f"Unexpected system error: {str(e)}")
        return send_return_status(500, {"error": "Internal server error"})
    finally:
        if session:
            session.close()

def get_play_ad(event, session):
    try:
        params = GetDivSchema(**(event.get("queryStringParameters") or {}))
        now_utc=datetime.now(timezone.utc)
        booking_formatted_date = now_utc.strftime("%Y-%m-%d")
       
        booking_formatted_date="2026-04-01"
        
        
        print("booking_date:",booking_formatted_date)
        
        if now_utc.minute < 30:
            # Any time from 00:00 to 00:29 falls into the 00:00 slot
            slot_hour = now_utc.strftime("%H:00")
        else:
            # Any time from 00:30 to 00:59 falls into the 00:30 slot
            slot_hour = now_utc.strftime("%H:30")
            print("Current Hour:", slot_hour)

        slot_hour="11:00"   

        # Database Query with targeted Exception handling
        query = (
            session.query(SlotBookingRequest.url)
            .join(CalendarTimeRates, SlotBookingRequest.booking_date_id == CalendarTimeRates.id)
            .filter(
                CalendarTimeRates.booking_date == booking_formatted_date,
                CalendarTimeRates.hour == slot_hour,
                SlotBookingRequest.booking_status == "c",
                SlotBookingRequest.division_id == params.divisionId
                
            )
            .limit(1)
        )
        
        s3_key = query.scalar()

        if not s3_key:
            logger.warning(f"No approved content found for date: {booking_formatted_date} hour: {slot_hour}")
            return send_return_status(404, {"message": "No approved content for this slot"})

        # S3 Presigned URL Generation
        try:
            url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET_NAME, "Key": s3_key},
                ExpiresIn=300
            )
        except ClientError as e:
            logger.error(f"S3 Presigned URL generation failed: {e}")
            return send_return_status(500, {"error": "Failed to generate access URL"})

        return send_return_status(200, {
            "url": url

        })

    except Exception as e:
        logger.error(f"Error in logic processing: {str(e)}")
        raise # Re-raise to be caught by lambda_handler wrapper
