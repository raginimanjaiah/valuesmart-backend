import os
import json
import boto3
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ValidationError,ConfigDict
from sqlalchemy import create_engine, Column, Integer, String, JSON, Enum, Index, func, select, Boolean, Date, ForeignKey,BigInteger
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy import literal


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
lambda_client = boto3.client('lambda')
BUCKET_NAME = "valuesmart"

# --- 2. ORM MODELS ---

class EquipmentCapabilitiesFct(Base):
    __tablename__ = "equipment_capabilities_fct"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_id = Column(Integer)
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

class BuyerProfile(Base):
    __tablename__ = 'buyers_profile'
    __table_args__ = {'schema': 'valuesmart'}
    id =  Column(BigInteger, primary_key=True, autoincrement=True)
    buyer_id = Column(BigInteger, nullable=False, unique=True)
    # Required Fields
    buyer_name = Column(String(150), nullable=False)
    country = Column(String(100), nullable=False)
    city_district = Column(String(120), nullable=False)
    pin_code = Column(String(20), nullable=False)
    mobile_number = Column(String(20), nullable=False)
    company_name = Column(String(200), nullable=True)
    phone_direct = Column(String(20), nullable=True)
    phone_board = Column(String(20), nullable=True)    

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

class BuyerProfileCreate(BaseModel):
    # This allows the model to work smoothly with SQLAlchemy objects later
    model_config = ConfigDict(from_attributes=True)
    buyer_id: int
    buyer_name: str = Field(..., max_length=150)
    country: str = Field(..., max_length=100)
    city_district: str = Field(..., max_length=120)
    pin_code: str = Field(..., max_length=20)
    mobile_number: str = Field(..., max_length=20)  
    # Optional fields
    company_name: Optional[str] = Field(None, max_length=200)
    phone_direct: Optional[str] = Field(None, max_length=20)
    phone_board: Optional[str] = Field(None, max_length=20)    

class QuestionnaireEquipmentMap(Base):
    __tablename__ = "questionnaire_Equipment_map"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    questionnaire_id = Column(Integer, nullable=False)
    machine_id = Column(Integer, nullable=False)    
    
class BuyerProfileUpdate(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    # Required: User MUST send this in the JSON
    buyer_id: int 
    # Optional: User can leave these out of the JSON
    buyer_name: Optional[str] = Field(None, max_length=150)
    company_name: Optional[str] = Field(None, max_length=200)
    country: Optional[str] = Field(None, max_length=100)
    city_district: Optional[str] = Field(None, max_length=120)
    pin_code: Optional[str] = Field(None, max_length=20)
    mobile_number: Optional[str] = Field(None, max_length=20)
    phone_direct: Optional[str] = Field(None, max_length=20)
    phone_board: Optional[str] = Field(None, max_length=20)


# --- 4. UTILITIES ---

def send_response(status_code: int, body: Any):
    return {
        "statusCode": status_code,
        "headers": {
             "Access-Control-Allow-Origin": "*",
             "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT"
            },
        "body": json.dumps(body)
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
        # Subquery: one questionnaire per machine_id
        questionnaire_subq = (
            select(
                QuestionnaireEquipmentMap.machine_id.label("machine_id"),
                func.max(QuestionnaireEquipmentMap.questionnaire_id).label("questionnaire_id")
                )
            .group_by(QuestionnaireEquipmentMap.machine_id)
            .subquery()
                     )        

        stmt = select(
            literal(None).label("supplier_id"), 
            EquipmentCapabilitiesFct.market_segment_id,
            EquipmentCapabilitiesFct.market_segment_name,
            EquipmentCapabilitiesFct.unit_operation_id,
            EquipmentCapabilitiesFct.unit_operation,
            EquipmentCapabilitiesFct.machine_id,
            EquipmentDetails.machine_name,
            EquipmentDetails.machine_image_url,
            questionnaire_subq.c.questionnaire_id
        ).join(EquipmentDetails, EquipmentDetails.id == EquipmentCapabilitiesFct.machine_id
        ).where(
            EquipmentCapabilitiesFct.division_id == params.divisionId,
            EquipmentCapabilitiesFct.market_segment_id == params.marketSegmentId,
            EquipmentCapabilitiesFct.unit_operation_id == params.unitOperationId
        ).outerjoin(
            questionnaire_subq,
            EquipmentCapabilitiesFct.machine_id == questionnaire_subq.c.machine_id   # <-- join on machine_id
        ).distinct()
        
        records = session.execute(stmt).mappings().all()
        results = [{**dict(r), "machine_image_url": get_s3_url(r.machine_image_url)} for r in records]
        return send_response(200, {"response": results})
    except ValidationError as e:
        return send_response(400, {"error": e.errors()})
    except Exception as e:
        session.rollback()
        return send_response(500, {"error": str(e)})        


def post_buyer_enquiry(event, session):
    try:
        body = json.loads(event.get("body", "{}"))
        data = PostEnquirySchema(**body)
        
        # 1. Save the Enquiry to MySQL
        new_record = BuyerEnquiredEquipment(**data.model_dump())
        new_record.e_registered_details = body
        session.add(new_record)
        session.commit() # ID is generated here
        
        # 2. Fire-and-Forget: Trigger the Match Lambda
        # This keeps the UI fast for the buyer
        lambda_client.invoke(
            FunctionName="match_and_notify",
            InvocationType='Event', # 'Event' makes it asynchronous
            Payload=json.dumps({"enquiry_id": new_record.id})
        )

        return send_response(201, {
            "message": "Enquiry submitted", 
            "id": new_record.id
        })

    except (ValidationError, json.JSONDecodeError) as e:
        return send_response(400, {"error": "Invalid Input"})
    except Exception as e:
        session.rollback()
        return send_response(500, {"error": str(e)})

def post_buyer_profile(event, session):
    try:
        # Step A: Parse the incoming JSON body from the event
        body = json.loads(event.get('body', '{}'))
        validated_data = BuyerProfileCreate(**body) 
        
        # Step C: Create SQLAlchemy instance
        # .model_dump() converts the Pydantic object into a clean Python dict
        new_profile = BuyerProfile(**validated_data.model_dump())
        session.add(new_profile)
        session.commit()
        return send_response(201, {"message": "Buyer profile created", "id": new_profile.id})
    except ValidationError as e:
        # Handles validation errors (e.g., missing required field)
        return send_response(400, {"error": "Validation Error", "details": e.errors()})
    except Exception as e:
        # General error handling
        print(e)
        session.rollback()
        return send_response(500, {"error": "Internal Server Error"})

def patch_buyer_profile(event, session):   
    try:
        body = json.loads(event.get('body', '{}'))
        update_data = BuyerProfileUpdate(**body)   
        profile_id = update_data.buyer_id 
        existing = session.query(BuyerProfile).filter(BuyerProfile.id == profile_id).first()
        if not existing:
            return send_response(404, {"error": "Not found"})

        for key, value in update_data.dict(exclude_unset=True).items():
            setattr(existing, key, value)           
        session.commit()
        return send_response(200, {"message": "Updated"})
    except Exception as e:
        session.rollback()
        return send_response(500, {"error": str(e)})


def get_buyer_profile(event, session):
    try:
        query_params = event.get('queryStringParameters') or {}
        
        # 1. Extract and Validate buyer_id (Required)
        buyer_id = query_params.get('buyer_id')
        if not buyer_id:
            return send_response(400, {"error": "buyer_id is required"})

        # # 2. Handle Pagination (Safety limits)
        # limit = min(int(query_params.get('limit', 20)), 100) # Max 100
        # offset = int(query_params.get('offset', 0))

        # 3. Query with Filter
        # .filter(BuyerProfile.buyer_id == buyer_id) is the critical addition here
        # profiles = (
        #     session.query(BuyerProfile)
        #     .filter(BuyerProfile.buyer_id == buyer_id)
        #     .order_by(BuyerProfile.created_at.desc())
        #     .limit(limit)
        #     .offset(offset)
        #     .all()
        # )
        profiles = (
            session.query(
                BuyerProfile.id,
                BuyerProfile.buyer_id,
                BuyerProfile.buyer_name,
                BuyerProfile.company_name,
                BuyerProfile.country,
                BuyerProfile.city_district,
                BuyerProfile.pin_code,
                BuyerProfile.mobile_number,
                BuyerProfile.phone_direct,
                BuyerProfile.phone_board
            )
    .filter(BuyerProfile.buyer_id == buyer_id)
    .all()
        )

        # 4. Serialize
        result = [BuyerProfileUpdate.model_validate(p).model_dump() for p in profiles]
        return send_response(200, {"response": json.dumps(result)})

    # except ValueError:
    #     return {"statusCode": 400, "body": json.dumps({"error": "Invalid limit or offset"})}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": "Internal Server Error"})}        

# --- 6. MAIN ROUTER ---

def lambda_handler(event, context):
    method = event.get("httpMethod", "").lower()
    path = event.get("path", "").strip("/").replace("/", "_")
    action_key = f"{method}_{path}"
    
    actions = {
        "get_div_mktseg": get_div_mktseg,
        "get_div_mktseg_unitop": get_div_mktseg_unitop,
        "get_div_mktseg_unitop_equip": get_div_mktseg_unitop_equip,
        "get_div_mktseg_cache": get_div_mktseg_cache,
        "post_buyer_enquiry": post_buyer_enquiry,
        "post_buyer_profile": post_buyer_profile,
        "patch_buyer_profile": patch_buyer_profile,
        "get_buyer_profile": get_buyer_profile
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
