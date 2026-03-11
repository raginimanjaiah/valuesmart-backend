import os
import json
import logging
import boto3
import uuid
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List
from functools import wraps
from datetime import datetime

from pydantic import BaseModel, Field, ValidationError, field_validator
from sqlalchemy import create_engine, Column, Integer, String, Enum as SQLEnum, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from botocore.exceptions import ClientError
from botocore.config import Config

# ── CONFIGURATION ────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
Base = declarative_base()

BUCKET_NAME = os.environ.get("BUCKET_NAME")


DATABASE_URL = f"mysql+pymysql://{os.environ['DB_USER']}:{os.environ['DB_PASS']}@{os.environ['DB_HOST']}/{os.environ['DB_NAME']}"

engine = create_engine(DATABASE_URL, pool_recycle=1800, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
s3_client = boto3.client("s3", config=Config(signature_version='s3v4'))

# ── 1. MODELS & ENUMS ─────────────────────────────────────────────────────────

class CalendarTimeRates(Base):
    __tablename__ = "calendar_time_rates"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_date = Column(Date, nullable=False)
    hour = Column(String(5), nullable=False) # e.g. "14:30"
    slot_number = Column(Integer, nullable=False)
    is_booked = Column(Boolean, default=False)

# Updated SlotBookingRequest to match your schema
class SlotBookingRequest(Base):
    __tablename__ = "slot_booking_request"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_id = Column(String(100), nullable=False)
    advertiser_id = Column(Integer, nullable=False)
    booking_date_id = Column(Integer, ForeignKey("valuesmart.calendar_time_rates.id"))
    booking_status = Column(SQLEnum("o", "w", "c", "ex", name="booking_status_enum"))
    url = Column(String(500))
    upload_status = Column(String(20)) # Added to track migration state

class UploadStatus(str, PyEnum):
    pending = 'pending'
    uploaded = 'uploaded'
    approved = 'approved'
    rejected = 'rejected'

class MediaMixin:
    id = Column(Integer, primary_key=True, autoincrement=True)
    media_type = Column(String(100), nullable=False) # e.g. image/png
    s3_key = Column(String(1024), nullable=False)
    media_upload_status = Column(SQLEnum(UploadStatus), default=UploadStatus.pending)
    file_hash = Column(CHAR(64),nullable=False) 
    created_at = Column(DateTime, default=datetime.utcnow)

class SupplierMedia(MediaMixin, Base):
    __tablename__ = 'supplier_media_assets'
    __table_args__ = {'schema': 'valuesmart'}
    supplier_id = Column(Integer, nullable=False)

class AdvertiserMedia(MediaMixin, Base):
    __tablename__ = 'advertiser_media_assets'
    __table_args__ = {'schema': 'valuesmart'}
    advertiser_id = Column(Integer, nullable=False)

# ── 2. SCHEMAS ───────────────────────────────────────────────────────────────

class MediaUploadRequest(BaseModel):
    id: int
    user_name: str
    file_type: str  # Content-Type
    file_name: str
    file_hash: str

class MediaUpdateSchema(BaseModel):
    asset_id: int
    user_name: str
    status: str

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v.lower() not in ['approved', 'rejected', 'pending']:
            raise ValueError("Invalid status")
        return v.lower()

# ── 3. AUTH & UTILS ──────────────────────────────────────────────────────────

def send_response(status_code: int, body: Any) -> dict:
    return {"statusCode": status_code, "headers": {"Access-Control-Allow-Origin": "*"}, "body": json.dumps(body, default=str)}

def validate_ownership(claims: dict, request_user: str) -> bool:
    if "admin" in claims.get("cognito:groups", []): return True
    return str(claims.get('cognito:username') or claims.get("sub")) == str(request_user)

def require_role(*allowed_roles):
    def decorator(func):
        @wraps(func)
        def wrapper(event, session):
            authorizer = event.get("requestContext", {}).get("authorizer", {})
            claims = authorizer.get("jwt", {}).get("claims", authorizer.get("claims", {}))
            groups = claims.get("cognito:groups", [])
            if not any(r in groups for r in allowed_roles) and "admin" not in groups:
                return send_response(403, {"error": "Forbidden"})
            return func(event, session, claims)
        return wrapper
    return decorator

def move_s3_object(old_key: str, new_key: str):
    s3_client.copy_object(Bucket=BUCKET_NAME, CopySource={'Bucket': BUCKET_NAME, 'Key': old_key}, Key=new_key)
    s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_key)

# ── 4. HANDLERS (DRY APPROACH) ───────────────────────────────────────────────

def handle_post(event, session, claims, model_class, entity_type: str):
    try:
        body = json.loads(event.get("body", "{}"))
        data = MediaUploadRequest(**body)
        
        if not validate_ownership(claims, data.user_name):
            return send_response(403, {"error": "Ownership mismatch"})

        # 1. Check for existing hash for this specific entity (e.g., supplier_id)
        # We dynamically filter based on the entity_type (supplier_id, user_id, etc.)
        entity_id_key = f"{entity_type}_id"
        existing_asset = session.query(model_class).filter(
            getattr(model_class, entity_id_key) == data.id,
            model_class.file_hash == data.file_hash
        ).first()

        # 2. If it exists, don't create a new one; just return the existing ID
        if existing_asset:
            return send_response(200, {
                "message": "File already exists for this entity",
                "asset_id": existing_asset.id,
                "s3_key": existing_asset.s3_key,
                "status": "duplicate" 
            })

        # 3. If new, proceed with creation
        unique_id = uuid.uuid4()
        s3_key = f"{entity_type}/{data.id}/pending/{unique_id}.{data.file_name.split('.')[-1]}"
        
        asset_data = {
            entity_id_key: data.id, 
            "media_type": data.file_type, 
            "s3_key": s3_key, 
            "file_hash": data.file_hash 
        }
        
        asset = model_class(**asset_data)
        session.add(asset)
        session.commit() # Commit here to secure the ID before generating the URL

        # 4. Generate the Presigned URL for the new asset
        url = s3_client.generate_presigned_url(
            'put_object', 
            Params={
                'Bucket': BUCKET_NAME, 
                'Key': s3_key, 
                "ContentType": data.file_type
            }, 
            ExpiresIn=3600
        )
        
        return send_response(201, {"asset_id": asset.id, "upload_url": url})

    except Exception as e:
        session.rollback() # Always rollback on failure
        return send_response(400, {"error": str(e)})

def handle_patch(event, session, claims, model_class, entity_type: str):
    try:
        data = MediaUpdateSchema(**(event.get("queryStringParameters") or {}))
        if not validate_ownership(claims, data.user_name): return send_response(403, {"error": "Unauthorized"})

        asset = session.query(model_class).get(data.asset_id)
        if not asset: return send_response(404, {"error": "Record not found"})

        old_key = asset.s3_key
        new_key = old_key.replace("/pending/", f"/{data.status}/", 1)
        # parts = old_key.split("/")
        # new_key = f"{entity_type}/{parts[1]}/{data.status}/{parts[-1]}"

        if old_key == new_key: return send_response(200, {"message": "No change needed"})

        # Step 1: Move File in S3
        move_s3_object(old_key, new_key)

        # Step 2: Update DB with Rollback Logic
        try:
            asset.s3_key = new_key
            asset.media_upload_status = UploadStatus[data.status]
            session.commit()
        except Exception as db_err:
            session.rollback()
            move_s3_object(new_key, old_key) # Compensating Transaction
            return send_response(500, {"error": "Database error, S3 rolled back"})

        return send_response(200, {"message": f"Moved to {data.status}", "path": new_key})
    except Exception as e:
        return send_response(400, {"error": str(e)})


def handle_get(event, session, claims, model_class, entity_type: str):
    try:
        # Extract entity_id from query params or path
        params = event.get("queryStringParameters") or {}
        if not validate_ownership(claims, data.user_name): return send_response(403, {"error": "Unauthorized"})
        entity_id = params.get("id")
        
        if not entity_id:
            return send_response(400, {"error": "Missing id parameter"})

        entity_id_key = f"{entity_type}_id"
        
        # Query all approved assets for this entity
        assets = session.query(model_class).filter(
            getattr(model_class, entity_id_key) == entity_id,
            model_class.media_upload_status == UploadStatus.approved
        ).all()

        results = []
        for asset in assets:
            # Generate a GET URL valid for 1 hour
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': BUCKET_NAME,
                    'Key': asset.s3_key
                },
                ExpiresIn=3600
            )
            
            results.append({
                "asset_id": asset.id,
                "file_name": asset.s3_key.split('/')[-1],
                "media_type": asset.media_type,
                "url": presigned_url,
                "created_at": asset.created_at
            })

        return send_response(200, {"count": len(results), "files": results})

    except Exception as e:
        logger.error(f"Error fetching assets: {str(e)}")
        return send_response(500, {"error": "Internal Server Error"})  


def confirm_advertiser_slot(event, session, claims):
    try:
        body = json.loads(event.get("body", "{}"))
        if not validate_ownership(claims, data.user_name): return send_response(403, {"error": "Unauthorized"})
        booking_id = body.get("booking_id")
        force_update = body.get("force_update", False) # New flag from frontend

        if not booking_id:
            return send_response(400, {"error": "Missing booking_id"})

        # 1. FETCH & LOCK
        result = session.query(SlotBookingRequest, CalendarTimeRates, AdvertiserMedia).\
            join(CalendarTimeRates, SlotBookingRequest.booking_date_id == CalendarTimeRates.id).\
            join(AdvertiserMedia, SlotBookingRequest.advertiser_id == AdvertiserMedia.advertiser_id).\
            filter(SlotBookingRequest.booking_id == booking_id).\
            filter(AdvertiserMedia.media_upload_status == UploadStatus.approved).\
            with_for_update(of=SlotBookingRequest).\
            first()

        if not result:
            return send_response(404, {"error": "Booking record or approved media not found"})

        booking, calendar, asset = result

        # 2. PREPARE S3 PATHS
        date_folder = calendar.booking_date.strftime("%Y-%m-%d")
        time_folder = calendar.hour.replace(":", "-") 
        file_name = asset.s3_key.split('/')[-1]
        
        old_key = asset.s3_key
        new_key = f"{date_folder}/{time_folder}/{file_name}"

        # 3. CHECK IF OBJECT ALREADY EXISTS IN THIS SLOT
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key)
            # If we reach here, the file exists
            if not force_update:
                return send_response(409, {
                    "error": "Media already present for this slot",
                    "message": "Do you want to replace the existing media?",
                    "existing_path": new_key
                })
            else:
                # User wants to replace; delete the old file in the slot first
                logger.info(f"Replacing existing media at {new_key}")
                s3_client.delete_object(Bucket=BUCKET_NAME, Key=new_key)
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise e # Real error, not just a missing file

        # 4. EXECUTE MOVE
        try:
            s3_client.copy_object(
                Bucket=BUCKET_NAME,
                CopySource={'Bucket': BUCKET_NAME, 'Key': old_key},
                Key=new_key
            )
            s3_client.delete_object(Bucket=BUCKET_NAME, Key=old_key)

            # 5. UPDATE DATABASE
            booking.url = new_key
            booking.upload_status = "confirmed_and_moved"
            asset.s3_key = new_key 
            
            session.commit()
            
            return send_response(200, {
                "message": "Slot confirmed and media updated",
                "new_path": new_key
            })

        except Exception as s3_or_db_err:
            session.rollback()
            logger.error(f"Migration failed: {str(s3_or_db_err)}")
            return send_response(500, {"error": "Critical error during file migration"})

    except Exception as e:
        session.rollback()
        return send_response(400, {"error": str(e)})
# ── 5. ROUTING ───────────────────────────────────────────────────────────────

@require_role("supplier")
def post_supplier(e, s, c): return handle_post(e, s, c, SupplierMedia, "supplier")

@require_role("advertiser")
def post_advertiser(e, s, c): return handle_post(e, s, c, AdvertiserMedia, "advertiser")

@require_role("admin")
def patch_supplier(e, s, c): return handle_patch(e, s, c, SupplierMedia, "supplier")

@require_role("admin")
def patch_advertiser(e, s, c): return handle_patch(e, s, c, AdvertiserMedia, "advertiser")

@require_role("advertiser")
def confirm_advertiser_slot(e, s, c): return handle_confirm_slot_and_move_file(e, s, c)

    

ROUTES = {
    "POST_media_supplier": post_supplier, 
    "POST_media_advertiser": post_advertiser,
    "PATCH_media_supplier": patch_supplier, 
    "PATCH_media_advertiser": patch_advertiser,
    "GET_media_supplier": get_supplier_assets,
    "GET_media_advertiser": get_advertiser_assets,
    "POST_media_confirm_slot": confirm_advertiser_slot
}

def lambda_handler(event, context):
    ctx = event.get("requestContext", {}).get("http", {})
    action = f"{ctx.get('method', '').upper()}_{event.get('path', '').strip('/').replace('/', '_')}"
    
    handler = ROUTES.get(action)
    if not handler: return send_response(404, {"error": "Not Found"})

    session = SessionLocal()
    try:
        return handler(event, session)
    finally:
        session.close()
