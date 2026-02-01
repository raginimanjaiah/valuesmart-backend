import os
import json
import boto3
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import create_engine, Column, Integer, String, JSON, Enum, Index, func, select, Boolean, Date, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.mysql import TIMESTAMP

# --- 1. GLOBAL INITIALIZATION (Optimization for Warm Starts) ---
Base = declarative_base()

# Database Config - Fetched from Environment Variables
# DB_USER = os.environ.get("DB_USER")
# DB_PASS = os.environ.get("DB_PASS")
# DB_HOST = os.environ.get("DB_HOST")
# DB_NAME = os.environ.get("DB_NAME")

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# Engine with connection pooling best practices
engine = create_engine(
    DATABASE_URL, 
    pool_recycle=3600, 
    pool_pre_ping=True, 
    pool_size=10, 
    max_overflow=2
)
SessionLocal = sessionmaker(bind=engine)

# AWS Clients initialized once
s3_client = boto3.client("s3")
BUCKET_NAME = "valuesmart"

# --- 2. ORM MODELS ---

class EquipmentCapabilitiesFct(Base):
    __tablename__ = "equipment_capabilities_fct"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    unit_operation_id = Column(Integer)
    unit_operation = Column(String(50))
    market_segment_id = Column(Integer) # Ensure this is INT for efficient joins
    market_segment_name = Column(String(50))
    division_id = Column(Integer)
    division_name = Column(String(250))

class EquipmentDetails(Base):
    __tablename__ = "equipments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True)
    machine_name = Column(String(250))
    machine_image_url = Column(String(500))

class MarketSegment(Base):
    __tablename__ = "market_segments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True)
    image_url = Column(String(500))

class BuyerEnquiredEquipment(Base):
    __tablename__ = 'buyer_enquired_equipments'
    __table_args__ = (
        Index('idx_buyer_match', 'equipment_id', 'market_segment_id', 'capacity_id', 'archive'),
        {'schema': 'valuesmart'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    buyer_id = Column(Integer, nullable=False)
    market_segment_id = Column(Integer, nullable=False)
    unit_operation_id = Column(Integer, nullable=False)
    equipment_id = Column(Integer, nullable=False)
    capacity_id = Column(Integer, nullable=True)
    e_registered_details = Column(JSON, nullable=True)
    archive = Column(Enum('Y', 'N'), server_default='N')

# --- 3. PYDANTIC SCHEMAS (Validation Layer) ---

class GetUnitOpSchema(BaseModel):
    divisionId: int
    marketSegmentId: int

class GetEquipSchema(GetUnitOpSchema):
    unitOperationId: int

class PostEnquirySchema(BaseModel):
    buyer_id: int
    market_segment_id: int
    unit_operation_id: int
    equipment_id: int
    capacity_id: Optional[int] = None
    e_registered_details: Optional[Dict[str, Any]] = None

# --- 4. UTILITIES ---

def send_response(status_code: int, body: Any):
    return {
        'statusCode': status_code,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        },
        'body': json.dumps(body)
    }

def get_s3_url(object_key: str, expires: int = 300):
    if not object_key: return None
    return s3_client.generate_presigned_url(
        'get_object', Params={'Bucket': BUCKET_NAME, 'Key': object_key}, ExpiresIn=expires
    )

# --- 5. ACTION HANDLERS ---

def get_div_mktseg_cache(event, session):
    try:
        S3_KEY = "division_marketsegment.json"
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_KEY)
        data = json.loads(response["Body"].read().decode("utf-8"))
        for division in data:
            for ms in division.get("marketSegments", []):
                if ms.get("imageUrl"):
                    ms["imageUrl"] = get_s3_url(ms["imageUrl"], expires=60)
        return send_response(200, {"response": data})
    except Exception as e:
        return send_response(404, {"error": "Cache not found or invalid"})

def get_div_mktseg(event, session):
    stmt = select(
        EquipmentCapabilitiesFct.division_id,
        EquipmentCapabilitiesFct.division_name,
        EquipmentCapabilitiesFct.market_segment_id,
        EquipmentCapabilitiesFct.market_segment_name,
        MarketSegment.image_url
    ).join(MarketSegment, EquipmentCapabilitiesFct.market_segment_id == MarketSegment.id).distinct()
    
    records = session.execute(stmt).mappings().all()
    results = [{**dict(r), "imageUrl": get_s3_url(r.image_url)} for r in records]
    return send_response(200, {"response": results})

def get_div_mktseg_unitop(event, session):
    try:
        params = GetUnitOpSchema(**(event.get("queryStringParameters") or {}))
        stmt = select(
            EquipmentCapabilitiesFct.division_id,
            EquipmentCapabilitiesFct.division_name,
            EquipmentCapabilitiesFct.unit_operation_id,
            EquipmentCapabilitiesFct.unit_operation
        ).where(
            EquipmentCapabilitiesFct.division_id == params.divisionId,
            EquipmentCapabilitiesFct.market_segment_id == params.marketSegmentId
        ).distinct()
        
        records = session.execute(stmt).mappings().all()
        return send_response(200, {"response": [dict(r) for r in records]})
    except ValidationError as e:
        return send_response(400, {"error": e.errors()})

def get_div_mktseg_unitop_equip(event, session):
    try:
        params = GetEquipSchema(**(event.get("queryStringParameters") or {}))
        stmt = select(
            EquipmentCapabilitiesFct.division_id,
            EquipmentDetails.machine_name,
            EquipmentDetails.id,
            EquipmentDetails.machine_image_url
        ).join(EquipmentDetails, EquipmentDetails.id == EquipmentCapabilitiesFct.id).where(
            EquipmentCapabilitiesFct.division_id == params.divisionId,
            EquipmentCapabilitiesFct.market_segment_id == params.marketSegmentId,
            EquipmentCapabilitiesFct.unit_operation_id == params.unitOperationId
        ).distinct()
        
        records = session.execute(stmt).mappings().all()
        results = [{**dict(r), "machineImageUrl": get_s3_url(r.machine_image_url)} for r in records]
        return send_response(200, {"response": results})
    except ValidationError as e:
        return send_response(400, {"error": e.errors()})

def post_buyer_enquiry(event, session):
    try:
        body = json.loads(event.get("body", "{}"))
        data = PostEnquirySchema(**body)
        new_record = BuyerEnquiredEquipment(**data.model_dump())
        session.add(new_record)
        session.commit()
        return send_response(201, {"message": "Enquiry submitted", "id": new_record.id})
    except (ValidationError, json.JSONDecodeError) as e:
        return send_response(400, {"error": "Invalid Input"})

# --- 6. MAIN ROUTER ---

def lambda_handler(event, context):
    method = event.get("httpMethod", "").lower()
    path = event.get("path", "").strip("/").replace("/", "_")
    action_key = f"{method}_{path}"
    
    actions = {
        "get_get_div_mktseg": get_div_mktseg,
        "get_get_div_mktseg_unitop": get_div_mktseg_unitop,
        "get_get_div_mktseg_unitop_equip": get_div_mktseg_unitop_equip,
        "get_get_div_mktseg_cache": get_div_mktseg_cache,
        "post_buyer_enquiry": post_buyer_enquiry
    }

    handler = actions.get(action_key)
    if not handler:
        return send_response(404, {"error": "Route not found"})

    session = SessionLocal()
    try:
        return handler(event, session)
    except Exception as e:
        session.rollback()
        print(f"Internal Error: {e}")
        return send_response(500, {"error": "Internal Server Error"})
    finally:
        session.close()