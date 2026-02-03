import os
import json
import logging
import datetime
import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Import your models
from models import SlotBookingRequest, CalendarTimeRates

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
        now = datetime.datetime.now()
        booking_date = now.date()
        hour = now.strftime("%H:00")

        # Database Query with targeted Exception handling
        query = (
            session.query(SlotBookingRequest.url)
            .join(CalendarTimeRates, SlotBookingRequest.booking_date_id == CalendarTimeRates.id)
            .filter(
                CalendarTimeRates.booking_date == booking_date,
                CalendarTimeRates.hour == hour,
                SlotBookingRequest.approval_status == "approved"
            )
            .limit(1)
        )
        
        s3_key = query.scalar()

        if not s3_key:
            logger.warning(f"No approved content found for date: {booking_date} hour: {hour}")
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
            "url": url, 
            "timestamp": now.isoformat(),
            "slot": hour
        })

    except Exception as e:
        logger.error(f"Error in logic processing: {str(e)}")
        raise # Re-raise to be caught by lambda_handler wrapper