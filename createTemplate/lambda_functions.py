import os
import json
from sqlalchemy import Column, Integer, String,Text, Boolean, ForeignKey, text,JSON,Enum,select,Date, DateTime,DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker, aliased
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
from sqlalchemy import func
import json
import boto3
import os
import uuid
from datetime import datetime
from sqlalchemy import update
from botocore.config import Config


Base = declarative_base()


# ORM Models

class MachineDetails(Base):
    __tablename__ = "equipments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    machine_name = Column(String(250))
    machine_image_url = Column(String(500))
    level_id = Column(Integer)
    uploadstatus= Column(String(10))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )

class UnitOperation(Base):
    __tablename__ = "unit_operations"
    __table_args__ = {"schema": "valuesmart"}
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    id = Column(Integer, primary_key=True, autoincrement=True)
    unit_operation = Column(String(250))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )
    

class MarketSegment(Base):
    __tablename__ = "market_segments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    market_segment_name = Column(String(250))
    image_url = Column(String(500))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )


class DimensionLookup(Base):
    __tablename__ = "dimension_lookup"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_name = Column(String(250))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )


class MasterListDim(Base):
    __tablename__ = "custom_lists"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer, nullable=False)
    dim_name = Column(String(255), nullable=False)
    dim_value = Column(String(255))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    ) 
   

class QuestionnaireEquipmentMap(Base):
    __tablename__ = "questionnaire_Equipment_map"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    questionnaire_id = Column(Integer, nullable=False)
    machine_id = Column(Integer, nullable=False)
   


class Question(Base):
    __tablename__ = "question"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    questionnaire_id = Column(Integer, ForeignKey("valuesmart.questionnaire.id"), nullable=False)
    text = Column(String(255), nullable=False)
    text_type = Column(String(50), nullable=False)  # 'text', 'dropdown', 'radio', 'checkbox'
    option_list_values = Column(Integer, nullable=True)
    option_list_source = Column(Integer, nullable=True)
    required = Column(Boolean)
    question_order = Column(Integer)


class Questionnaire(Base):
    __tablename__ = "questionnaire"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False) 
    template_json=Column(JSON)
    is_generic = Column( Boolean, default=False, nullable=False)
    version = Column(Integer, nullable=False)

class Variants(Base):
    __tablename__ = "variants"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(250))
    variant = Column(String(250))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )  

class Capacity(Base):
    __tablename__ = "capacity"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(250))
    capacity = Column(String(250))  
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )

class Division(Base):
    __tablename__ = "division"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    division = Column(String(250))
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )
        

class LevelPoints(Base):
    __tablename__ = "level_points"
    __table_args__ = {"schema": "valuesmart"}  # replace with DB_NAME if needed
    id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String(100))
    points = Column(Integer)
    archive = Column(Enum('Y', 'N'), default='N')

class SupplierRegisteredEquipments(Base):
    __tablename__ = "supplier_registered_equipments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(Integer, nullable=False)
    market_segment_id = Column(Integer, nullable=False)
    unit_operation_id = Column(Integer, nullable=False)
    equipment_id = Column(Integer, nullable=False)
    capacity_id = Column(Integer, nullable=True)
    e_registered_details = Column(JSON, nullable=True)
    archive = Column(
        Enum("Y", "N", name="archive_enum"),
        default="N",
        nullable=False
    )    
      

# DB connection
# DB_USER = os.environ["DB_USER"]
# DB_PASS = os.environ["DB_PASS"]
# DB_HOST = os.environ["DB_HOST"]
# DB_NAME = os.environ["DB_NAME"]



# Initialize S3 outside the handler for connection reuse (Warm Starts)
# Use SigV4 for better performance and compatibility
s3_config = Config(signature_version='s3v4')
s3_client = boto3.client('s3', config=s3_config)


def createDataBaseConnection():
    try:
        engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}",
    pool_recycle=3600,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=2
)

        SessionLocal = sessionmaker(bind=engine)
        print(" database connection created")
        return SessionLocal
    except Exception as e:
        print("unable to create connection")    
        exit()

    
def s3Creation():
    try:
        print(" s3 connection started")
        s3 = boto3.client(
    "s3"
)    
        print(" s3 connection created")
        return s3
    except Exception as e:
        print("unable to s3 connection" , {e})    
        

class Supplier(Base):
    __tablename__ = "supplier"
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(255))
    role = Column(String(255))
    company_name = Column(String(255))
    country = Column(String(255))
    company_address = Column(String(500))
    company_number = Column(Integer)
    company_website = Column(String(500))
    contact_title = Column(String(3))
    contact_first_name = Column(String(255))
    contact_middle_name = Column(String(255))
    contact_last_name = Column(String(255))
    contact_designation = Column(String(255))
    contact_department = Column(String(255))
    contact_official_mobile_number = Column(String(20))
    contact_direct_mobile_number = Column(String(20))
    contact_official_email_id = Column(String(255))
    state = Column(String(255))
    city = Column(String(255))
    pincode = Column(String(255))
    e_registered_details = Column(JSON)
    e_registered_verified_status = Column(
        Enum("V", "N", "R", name="verified_status_enum"), default="N"
    )  
    
class CalendarTimeRate(Base):
    __tablename__ = "calendar_time_rates"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_date = Column(Date, nullable=False)
    hour = Column(String(5), nullable=False)
    slot_number = Column(Integer, nullable=False)
    rate_points = Column(Integer, nullable=False)
    is_booked = Column(Boolean, default=False) 

class SlotBookingRequest(Base):
    __tablename__ = "slot_booking_request"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(100))
    booking_date_id = Column(Integer, nullable=False)
    booking_status = Column(
        Enum("o", "w", "c", "ex", name="booking_status_enum"),
        nullable=False
    )
    waiting_position = Column(Integer, nullable=True)
    url = Column(String(500))
    approval_status = Column(
        Enum("pending", "approved", "rejected", name="approval_status_enum"),
        nullable=False
    )

# Authorization (Cognito)
# def authorize_user(event, required_role="admin"):
#     claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
#     groups = claims.get("cognito:groups", [])
#     if isinstance(groups, str):
#         groups = groups.split(",")
#     return required_role in groups

# Lambda Handler
def lambda_handler(event, context):
    # if not authorize_user(event, required_role="admin"):
    #     return {"statusCode": 403, "body": json.dumps({"error": "Forbidden – Admins only"})}
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims", {})

    # Extract some useful info from the JWT
    user_id = claims.get("sub")                 # Cognito's unique user ID (UUID)
    username = claims.get("cognito:username")   # Username in Cognito
    email = claims.get("email")                 # User's email
    groups = claims.get("cognito:groups")       
    SessionLocal=createDataBaseConnection()
    session = SessionLocal()
    print("the event is", event)
    method = event["httpMethod"] 
    methodLower = event["httpMethod"].lower()
    methodPath = event.get("path", "").replace('/', '')
    #create dict for apis
    actions = {
    "post_levels_points": post_levels_points,
    "get_levels_points": get_levels_points,
    "patch_levels_points": patch_levels_points,
    "patch_ad_approval" : patch_ad_approval,
    "patch_SupplierRegisteredEquipments": patch_SupplierRegisteredEquipments,
    "get_SupplierRegisteredEquipments": get_SupplierRegisteredEquipments
    }
    print(f"the method is join {methodLower}_{methodPath}")
    handler = actions.get(f"{methodLower}_{methodPath}")
    # if not handler:
    #     return {"statusCode": 405, "body": "Method not allowed"}
    if handler:
        return handler(event,session)
    else:
        print("check for the other method")    

    if method == "GET":
        print(" the selected method is GET")
        return getHandler(event,session)
    elif method == "POST": 
        print(" the selected method id POST")
        response=postHandler(event,session) 
        return(response)
    elif method == "DELETE":
        print(" the selected method is DELETE")
        return deleteHandler(event, session)  
    elif method == "PATCH":
        print(" the selected method is PATCH")
        return patchHandler(event, session)   


def patch_SupplierRegisteredEquipments(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        required_fields = ["id", "archive"]  
        metaDetails=queryStringParameters
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result        
        id = queryStringParameters.get("id")
        archive=queryStringParameters.get("archive")
        record = session.query(SupplierRegisteredEquipments).filter_by(id=id).first()
        if record:
            record.archive = archive
            session.commit()
            return send_return_status(200,json.dumps({"response":"archived successfully"})) 
        else:
            return send_return_status(500, json.dumps({"response": "no record found "}))
    except Exception as e:
        print(e)
        return send_return_status(400, json.dumps({"error": "Invalid request"}))  

def get_SupplierRegisteredEquipments(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        required_fields = ["supplier_id"]
        metaDetails=queryStringParameters
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result
        supplier_id = queryStringParameters.get("supplier_id")
        records = session.query(SupplierRegisteredEquipments).filter_by(supplier_id=supplier_id).all()
        if records:
            data = []
            for record in records:
                data.append({
                    "id": record.id,
                    "supplier_id": record.supplier_id,
                    "market_segment_id": record.market_segment_id,
                    "unit_operation_id": record.unit_operation_id,
                    "equipment_id": record.equipment_id,
                    "capacity_id": record.capacity_id,
                    "e_registered_details": record.e_registered_details,
                    "archive": record.archive
                })
            return send_return_status(200, json.dumps({"response": data}))
        else:
            return send_return_status(500, json.dumps({"response": "no record found "}))
    except Exception as e:
        print(e)
        return send_return_status(400, json.dumps({"error": "Invalid request"}))          

def patch_ad_approval(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))

        required_fields = ["entityId","bookingDateId","status"]  
        metaDetails=queryStringParameters
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result        
        entity_id = queryStringParameters.get("entityId")
        booking_date_id = queryStringParameters.get("bookingDateId")
        status = queryStringParameters.get("status")
        stm = (
            update(SlotBookingRequest)
            .where(SlotBookingRequest.id == entity_id,
              SlotBookingRequest.booking_date_id == booking_date_id)
            .values(approval_status=status)
        )
        result=session.execute(stm)
        session.commit()
        if result.rowcount == 0:
            return send_return_status(400, json.dumps({"error": "no record found"}))
        return send_return_status(200, json.dumps({"response": "Updated successfully"}))

    except Exception as e:
        print(e)
        return send_return_status(400, json.dumps({"error": "Invalid request"}))
      
def post_levels_points(event, session):
    body = event.get("body")
    if not body:
        return send_return_status(400, json.dumps({"error": "Request body is missing"}))

    # Parse JSON safely
    try:
        metaDetails = json.loads(body)
    except Exception:
        return send_return_status(400, json.dumps({"error": "Invalid JSON format"}))

    # Validate 'level' structure
    levels = metaDetails.get("level")
    if not isinstance(levels, list) or not levels:
        return send_return_status(400, json.dumps({"error": "Field 'level' must be a non-empty list"}))

    # Efficient validation in one pass
    invalid_entries = [
        f"Item {i}: missing {', '.join(missing)}"
        for i, lvl in enumerate(levels, start=1)
        if not isinstance(lvl, dict)
        or (missing := [f for f in ("level", "points") if f not in lvl or lvl[f] is None])
    ]

    if invalid_entries:
        return send_return_status(400, json.dumps({"error": invalid_entries}))

    # Prepare all items for bulk insert
    bulk_items = [
        LevelPoints(level=lvl["level"], points=lvl["points"])
        for lvl in levels
    ]

    # Bulk insert for performance
    try:
        session.bulk_save_objects(bulk_items)
        session.commit()
    except Exception as e:
        session.rollback()
        return send_return_status(500, json.dumps({"error": f"Database error: {str(e)}"}))
    return send_return_status(200, json.dumps({"response": "Loaded successfully"}))

def get_levels_points(event, session):
    archive = "N"
    try:       
        results = session.query(LevelPoints).all()
        data = [
        {"id": r.id, "level": r.level, "points": r.points, "archive": r.archive}
        for r in results
        ]

        if not data:
            data=[]
            return send_return_status(200, json.dumps(data))
        return send_return_status(200, json.dumps(data))  
    except Exception as e:
        print(e)
        return send_return_status(400, json.dumps({"error": "Invalid request"}))    

def patch_levels_points(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))

        required_fields = ["id", "archive"]  
        metaDetails=queryStringParameters
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result        
        id = queryStringParameters.get("id")
        archive=queryStringParameters.get("archive")
        record = session.query(LevelPoints).filter_by(id=id).first()
        if record:
            record.archive = archive
            session.commit()
            return send_return_status(200,json.dumps({"response":"archived successfully"})) 
        else:
            return send_return_status(500, json.dumps({"response": "no record found "}))
    except Exception as e:
        print(e)
        return send_return_status(400, json.dumps({"error": "Invalid request"}))

def check_required_fields(data, required_fields):
    missing = [field for field in required_fields if field not in data or data.get(field) is None]
    if missing:
        return send_return_status(
            400,
            json.dumps({"error": f"Missing required fields: {', '.join(missing)}"})
        )
    else:
        return True
        

def patchHandler(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters", {}) or {}
        path = event.get("path", "").lower()
        if path.startswith("/custom_list"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(MasterListDim).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200,json.dumps({"response":"archived successfully"})) 
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))                                                                    
        elif path.startswith("/capacity"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(Capacity).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))
        elif path.startswith("/division"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(Division).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))            
        elif path.startswith("/variants"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(Variants).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))
        elif path.startswith("/unit_operations"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(UnitOperation).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))       
        elif path.startswith("/market_segments"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            upload_status=queryStringParameters.get("uploadStatus")

            # ✅ id is mandatory
            if not id:
                return send_return_status(400, json.dumps({"error": "id query parameter is required"}))
                
            # ✅ at least one of archive or uploadStatus must be present
            if archive is None and upload_status is None:
                return send_return_status(400, json.dumps({ "error": "Either 'archive' or 'uploadStatus' query parameter is required" }))       
   
            if id and archive :
                record = session.query(MarketSegment).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))
            if id and upload_status :
                record = session.query(MarketSegment).filter_by(id=id).first()
                if record:
                    record.upload_status = upload_status
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"upload status updated successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))         
        elif path.startswith("/equipments"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            upload_status=queryStringParameters.get("uploadStatus")

            # ✅ id is mandatory
            if not id:
                return send_return_status(400, json.dumps({"error": "id query parameter is required"}))
                
            # ✅ at least one of archive or uploadStatus must be present
            if archive is None and upload_status is None:
                return send_return_status(400, json.dumps({ "error": "Either 'archive' or 'uploadStatus' query parameter is required" })) 
            if id and archive:
                record = session.query(MachineDetails).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))
            elif id and upload_status:
                record = session.query(MachineDetails).filter_by(id=id).first()
                if record:
                    record.upload_status = upload_status
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"upload status updated successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))
        
            else:
                body= event.get("body", {}) 
                metaDetails = json.loads(body)
                if metaDetails:  
                    id=metaDetails.get("id")
                    file_name=metaDetails.get("fileName")
                    file_type=metaDetails.get("fileType") 
                    s3=s3Creation()
                    presigned_url = s3.generate_presigned_url(
                  "put_object",
                  Params={"Bucket": BUCKET_NAME, "Key": file_name, "ContentType": file_type},
                  ExpiresIn=60
                    ) 
                    print("the presigned url is ", presigned_url)
                    record = session.query(MachineDetails).filter_by(id=id).first()
                    if record:
                        print("the record is ", record)
                        machine_image_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{file_name}"
                        record.machine_image_url=machine_image_url
                        session.commit()
                        return send_return_status(200, json.dumps({"response":"image uploaded successfully","uploadUrl":presigned_url}))
                    else:
                        return send_return_status(500, json.dumps({"response": "no record found "}))        

        elif path.startswith("/templates"):
            id = queryStringParameters.get("id")
            archive=queryStringParameters.get("archive")
            if id :
                record = session.query(Template).filter_by(id=id).first()
                if record:
                    record.archive = archive
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))                                                        
        elif path.startswith("/supplier_eregistration"):
            id = queryStringParameters.get("id")
            supplierStatus=queryStringParameters.get("supplier_status")
            if id :
                record = session.query(Supplier).filter_by(id=id).first()
                if record:
                    record.e_registered_verified_status = supplierStatus
                    session.commit()
                    return send_return_status(200, json.dumps({"response":"archived successfully"}))
                else:
                    return send_return_status(500, json.dumps({"response": "no record found "}))

    except Exception as e:
        print(" error in patch handler:", e  )
        return send_return_status(500, json.dumps({"error": "encounterd some issue put command not executed"}))

def getHandler(event,session):
    try:
        path = event.get("path", "").lower()
        if path.startswith("/custom_list"):
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" i am in option set")
            if  queryStringParameters:
                dim_name = queryStringParameters.get("dim_name")
                data =  session.query(MasterListDim).filter(MasterListDim.dim_name == dim_name).all()
                result = []
                for r in data:
                    result.append({"id": r.id,
                    "dim_id": r.dim_id,
                    "dim_name": r.dim_name,
                    "dim_value": r.dim_value,
                    "archive": r.archive })             
                print(" the result is ", result)
                return send_return_status(200, json.dumps(result))
        elif path.startswith("/all_custom_list"): 
            print(" i am in all_custom_list")
            data =  session.query(MasterListDim).all()
            result = []
            for r in data:
                result.append({"id": r.id,
                "dim_id": r.dim_id,
                "dim_name": r.dim_name,
                "dim_value": r.dim_value,
                "archive": r.archive})
            print(" the result is ", result)
            return send_return_status(200, json.dumps(result))      
        elif path.startswith("/capacity"):
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" i am in capacity_list_dim set")
            if  queryStringParameters["dim_name"]:
                dim_name = queryStringParameters.get("dim_name")
                result=[]
                #data =  session.query(Capacity).filter(Capacity.dim_name == dim_name).all()
                stmt = select(
                            Capacity.id,
                            Capacity.dim_id,
                            Capacity.dim_name,
                            Capacity.capacity.label('dim_value'),
                            Capacity.archive
                            ).filter(Capacity.dim_name == dim_name)
                result = [dict(row._mapping) for row in session.execute(stmt)]
                # for r in data:
                #     result.append({
                #             "id": r.id,
                #             "dim_id": r.dim_id,
                #             "dim_name": r.dim_name,
                #             "dim_value": r.capacity,
                #             "archive": r.archive
                #             })
                return send_return_status(200,json.dumps(result))              
        elif path.startswith("/equipments") :
            queryStringParameters = event.get("queryStringParameters") or {}
            dim_name = queryStringParameters.get("dim_name")
    
            if not dim_name:
                return send_return_status(400, json.dumps({"error": "Missing dim_name parameter"}))

             # 1. High Performance Query: Using column-only selection (deferred loading)
            # This prevents SQLAlchemy from loading full model instances into memory
            data = (
                session.query(
                    MachineDetails.id.label("machine_id"),
                    MachineDetails.dim_id,
                    MachineDetails.dim_name,
                    MachineDetails.machine_name,
                    MachineDetails.machine_image_url,
                    MachineDetails.archive.label("machine_archive"),
                    LevelPoints.id.label("level_id"),
                    LevelPoints.level.label("level_name"),
                    LevelPoints.points.label("level_points")
                    )
                    .join(LevelPoints, MachineDetails.level_id == LevelPoints.id)
                    .filter(MachineDetails.dim_name == dim_name)
                    .filter(LevelPoints.archive == 'N')
                     .all()
                    )

            result = []
            for r in data:
                # 2. Generate Presigned URL
                # We only generate if the key exists to avoid broken links
                image_url = None
                if r.machine_image_url:
                    try:
                        image_url = s3_client.generate_presigned_url(
                            'get_object',
                            Params={'Bucket': BUCKET_NAME, 'Key': r.machine_image_url},
                            ExpiresIn=3600  # 1 Hour
                                )
                    except Exception as e:
                        send_return_status(500, json.dumps({"error": "Failed to generate presigned URL"}))
                        image_url = None

                result.append({
                        "id": r.machine_id,
                        "dim_id": r.dim_id,
                        "dim_name": r.dim_name,
                        "equipment": r.machine_name,
                        "image_url": image_url,
                        "archive": r.machine_archive,
                        "level_id": r.level_id,
                        "level_name": r.level_name,
                        "level_points": r.level_points
                        })

            return send_return_status(200, json.dumps(result))                      
        elif path.startswith("/unit_operations"):
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" i am in unit_segment_list_dim")
            if  queryStringParameters["dim_name"]:
                dim_name = queryStringParameters.get("dim_name")
                print(" the dim name is", dim_name)
                data =  session.query(UnitOperation).filter(UnitOperation.dim_name == dim_name).all()
                result=[]
                for r in data:
                    result.append({
                            "id": r.id,
                            "dim_id": r.id,
                            "dim_name":'unit_operations',
                            "dim_value": r.unit_operation,
                            "archive": r.archive
                            })
                return send_return_status(200,json.dumps(result))
        elif path.startswith("/market_segments"):
            query_params = event.get("queryStringParameters") or {}
            dim_name = query_params.get("dim_name")    
            if not dim_name:
                return send_return_status(400, json.dumps({"error": "dim_name is required"}))
            # 1. Column-specific query for maximum speed
            data = (
                session.query(
                MarketSegment.id,
                MarketSegment.market_segment_name,
                MarketSegment.archive,
                MarketSegment.image_url  
                )
            .filter(MarketSegment.dim_name == dim_name)
            .all()
                )
            result = []
            for r in data:
                presigned_url = None
                if r.image_url:
                    presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': r.image_url},
                    ExpiresIn=3600
                    )

                result.append({
                    "id": r.id,
                    "dim_id": r.id,
                    "dim_name": 'market_segment_details',
                    "dim_value": r.market_segment_name,
                    "archive": r.archive,
                    "image_url": presigned_url 
                    })

            return send_return_status(200, json.dumps(result))
            # queryStringParameters= event.get("queryStringParameters", {}) or {}
            # print(" i am in market_segment_list_dim")
            # if  queryStringParameters["dim_name"]:
            #     dim_name = queryStringParameters.get("dim_name")
            #     data =  session.query(MarketSegment).filter(MarketSegment.dim_name == dim_name).all()
            #     result=[]
            #     for r in data:
            #         result.append({
            #                 "id": r.id,
            #                 "dim_id": r.id,
            #                 "dim_name":'market_segment_details',
            #                 "dim_value": r.market_segment_name,
            #                 "archive": r.archive
            #                 })
            #     return send_return_status(200,json.dumps(result))                   
        elif path.startswith("/variants"):
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" i am in option set")
            if  queryStringParameters:
                dim_name = queryStringParameters.get("dim_name")
                data =  session.query(Variants).filter(Variants.dim_name == dim_name).all()
                result=[]
                for r in data:
                    result.append({
                            "id": r.id,
                            "dim_id": r.dim_id,
                            "dim_name": r.dim_name,
                            "dim_value": r.variant,
                            "archive": r.archive
                            })
                return send_return_status(200, json.dumps(result)) 
        elif path.startswith("/division"):
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" i am in division set")
            if  queryStringParameters:
                dim_name = queryStringParameters.get("dim_name")
                data =  session.query(Division).filter(Division.dim_name == dim_name).all()
                result=[]
                for r in data:
                    result.append({
                            "id": r.id,
                            "dim_id": r.dim_id,
                            "dim_name": r.dim_name,
                            "dim_value": r.division,
                            "archive": r.archive
                            })
                return send_return_status(200, json.dumps(result))                                   
        elif path.startswith("/all_master_list_dim"):
            print(" i am in master_dim_list_all")
            dims = session.query(MasterListDim.dim_name).distinct().all()
            result = []
            response={} 
            for (dim_name,) in dims:
                    print(" the dim name is", dim_name)
                    data =  session.query(MasterListDim).filter(MasterListDim.archive=='N').filter(MasterListDim.dim_name==dim_name).all()
                    for r in data:
                        result.append( r.dim_value)
                    response[dim_name]=result
                    result = [] 
            data =  session.query(Capacity).filter(Capacity.archive=='N').all() 
            for r in data:
                result.append(  r.capacity)                          
            response['capacity']=result
            result=[]
            data =  session.query(Variants).filter(Variants.archive=='N').all() 
            for r in data:
                result.append(r.variant)
            response['variant']=result 
            result=[]
            data =  session.query(UnitOperation).filter(UnitOperation.archive=='N').all()
            for r in data:
                result.append(r.unit_operation)
            response['unit_operations']=result 
            result=[]
            data =  session.query(MarketSegment).filter(MarketSegment.archive=='N').all()   
            for r in data:
                result.append(r.market_segment_name)                     
            response['market_segment_details']=result 
            return send_return_status(200, json.dumps(response))
        elif path.startswith("/questionnaire"):
            print(" i am in questionnaire")
            data =  session.query(Questionnaire).all()
            result = []
            for r in data:
                result.append({
                 "template_id": r.id,
                 "template_name": r.name,
                 "template_json": r.template_json,
                 "template_version": r.version,
                 "is_generic": r.is_generic
               })
            print("the result is ", result)
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            }
        elif path.startswith("/questions"):
            print(" i am in questions")
            data =  session.query(Question).all()
            result = []
            for r in data:
                result.append({
                 "question_id": r.id,
                 "questionnaire_id": r.questionnaire_id,
                 "text": r.text,
                 "text_type": r.text_type,
                 "master_list_id": r.master_list_id,
                 "master_list_dim_id": r.master_list_dim_id,
                 "required": r.required,
                 "question_order": r.question_order
               })
            print("the result is ", result)
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            }
        elif path.startswith("/questionnairequipmentmap"):
            result =  (session.query(
                    Questionnaire.id.label("questionnaire_id"),
                    Questionnaire.name.label("name"),
                    MachineDetails.id.label("machine_id"),
                    MachineDetails.machine_name,
                    MachineDetails.machine_image_url
    ).join(QuestionnaireEquipmentMap, Questionnaire.id == QuestionnaireEquipmentMap.questionnaire_id).join(MachineDetails, QuestionnaireEquipmentMap.machine_id == MachineDetails.id).all())

            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            } 
        elif path.startswith("/latest_templates") :
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            print(" the queryStringParameters are :",queryStringParameters["isGeneric"])
            if queryStringParameters and queryStringParameters["isGeneric"] == "true":
                    isGeneric=queryStringParameters["isGeneric"]
                    subq = (
                    session.query(
                    Questionnaire.name,
                    func.max(Questionnaire.id).label("max_version")
                    ).filter(Questionnaire.is_generic==True).group_by(Questionnaire.name).subquery()
                    )
            else:
                 subq = (
                    session.query(
                    Questionnaire.name,
                    func.max(Questionnaire.id).label("max_version")
                    ).group_by(Questionnaire.name).subquery()
                    )       

            Q2 = aliased(Questionnaire)

                # join to get full row of latest version
            latest_questionnaires = (
            session.query(Q2).join(subq, (Q2.name == subq.c.name) & (Q2.id == subq.c.max_version)).all())

            result = []
            for q in latest_questionnaires:
                print(q.id, q.name, q.version)
                result.append({
                     "template_id": q.id,
                     "template_name": q.name,
                     "is_generic": q.is_generic
                     }) 
            return send_return_status(200, json.dumps(result))         
        elif path.startswith("/template"): 
            print(" i am in preview template")
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            if queryStringParameters:
                template_id = queryStringParameters.get("template_id")
                result={}
                data =  session.query(Questionnaire).filter(Questionnaire.id == template_id).all()
                print("the json result is ", data)
                if data:
                    result={"template_id": data[0].id,
                            "template_name": data[0].name,
                            "template_json": data[0].template_json}

                    return send_return_status(200, json.dumps(result))
                else:
                    return send_return_status(200, json.dumps({"error": "no data found"}))    
        elif path.startswith("/supplier_eregistration"):
            print(" i am in supplier")
            queryStringParameters= event.get("queryStringParameters", {}) or {}
            if queryStringParameters and "supplier_id" in queryStringParameters:
                supplier_id = queryStringParameters.get("supplier_id")
                data =  session.query(Supplier.id, Supplier.user_name,Supplier.e_registered_details).filter(Supplier.id == supplier_id).first()
                if data:
                    result = dict(data._mapping) 
                    return send_return_status(200, json.dumps(result))
                else:
                    return send_return_status(200, json.dumps({"error": "no data found"}))
            else:
                data =  session.query(Supplier.id, Supplier.user_name,Supplier.e_registered_details).all()
                columns = ["id", "user_name", "e_registered_details"]
                results = [
                          {col: r._mapping[col] for col in columns}
                           for r in data
                          ]
                return send_return_status(200, json.dumps(results))        
        
                
    except Exception as e:
        print("print the error:", e)
        return send_return_status(500, json.dumps({"error": "issue in extracting  {e}"}))

def postHandler(event,session):
    try:
        path = event.get("path", "").lower()
        body= event.get("body", {}) or {}        
                        
        if path.startswith("/template"): 
            print(" i am in templates")
            metaDetails=json.loads(body)
            title=metaDetails.get("title")
            isGeneric=metaDetails.get("isGeneric")
            templateJson=metaDetails
            print("isGeneric", isGeneric)
            record = Questionnaire(name=title,is_generic=isGeneric,version=1,template_json=templateJson)  
            try:
                session.add(record)
                session.flush()
                questionnaire_id = record.id
                print("the questionnaire id is", questionnaire_id)
                questions=metaDetails["questions"]
                print("the questions are", questions)
  
                if isGeneric == False :
                    print("i am in generic")
                    equipmentList=metaDetails.get("assignedEquipment")
                    print("the equipment list is", equipmentList)
                    for id in equipmentList:
                        print("the equipmentlist is", id)
                        print("the questionnaire_id", questionnaire_id)
                        record = QuestionnaireEquipmentMap(questionnaire_id=questionnaire_id,machine_id=id)
                        session.add(record)
                    session.commit()
                return send_return_status(200, json.dumps({"success": f"successfully data loaded"}))    
            except Exception as e:
                    print("print the error:",e)
                    return send_return_status(500, json.dumps({"error": f"Error inserting into table"}))              
        elif path.startswith("/custom_list"):
            metaDetails=json.loads(body)
            dimName = list(metaDetails.keys())[0]
            dimValues=list(metaDetails.values())[0]
            print("the dim values are", dimValues)
            dim_id=get_dimension_id(dimName,session)
            if dim_id is None:
                dim_id = insert_dimension_id(dimName,session)

            for opt in dimValues:
                    print("the option is", opt)
                    record = MasterListDim(dim_id=dim_id,dim_name=dimName, dim_value=opt)
                    session.add(record)
                    session.commit()
            return send_return_status(200, json.dumps({"success": f"successfully data loaded"}))        
            
    except SQLAlchemyError as e:
        print("the error is ", e)
        return send_return_status(500, json.dumps({"error": f"Error inserting into table"}))
    finally:
        session.close()



def send_return_status(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PATCH"
        },
        'body': body
    }





def get_questionnaire_data(session, questionnaire_id=None, version=None):
    """
    SQLAlchemy query equivalent to your SQL query
    Returns data in the JSON format structure
    """
    print("the questionnaire id is", questionnaire_id)
    print("the version is", version)
    query = session.query(
        Questionnaire.name.label('name'),
        Questionnaire.is_generic.label('is_generic'),
        Questionnaire.version.label('version'),
        Question.text.label('text'),
        Question.text_type.label('type'),
        Question.required.label('required'),
        Question.question_order.label('order'),
        Question.master_list_id.label('master_list_id'),
        Question.master_list_dim_id.label('master_list_dim_id'),
        MasterListDim.dim_name.label('dim_name'),
        MasterListDim.dim_value.label('dim_value'),
        MachineDetails.id.label('equipment_id'),
        MachineDetails.machine_name.label('equipment_name')
    ).select_from(
        Questionnaire
    ).join(
        Question, Questionnaire.id == Question.questionnaire_id
    ).join(
        MasterListDim, 
        and_(
            Question.master_list_id == MasterListDim.id,
            Question.master_list_dim_id == MasterListDim.dim_id
        )
    ).outerjoin(
        QuestionnaireEquipmentMap, 
        Questionnaire.id == QuestionnaireEquipmentMap.questionnaire_id
    ).outerjoin(
        MachineDetails, 
        QuestionnaireEquipmentMap.machine_id == MachineDetails.id
    )
    
    if questionnaire_id:
        query = query.filter(Questionnaire.id == questionnaire_id).filter(Questionnaire.version == version)
    raw_results = query.all()
    return format_questionnaire_json(raw_results)



def get_dimension_id(dim_name,session):
    try:
        record = session.query(DimensionLookup).filter_by(dim_name=dim_name).first()
        if record is None:
            return None
        else:
            return  record.id  
    except SQLAlchemyError as e:
        return {"statusCode": 500,   "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PATCH"
        }, "body": json.dumps({"error": str(e)})}

def insert_dimension_id(dimName,session) :
    try:
        record = DimensionLookup(dim_name=dimName)
        session.add(record)
        session.commit()
        return record.id
    except SQLAlchemyError as e:
        return {"statusCode": 500,   "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PATCH"
        }, "body": json.dumps({"error": str(e)})}       

def format_questionnaire_json(raw_results):
    """
    Transform query results into the desired JSON format
    """
    print("the raw results are", raw_results)
    if not raw_results:
        return None
    
    # Group by questionnaire
    questionnaire_data = {}
    questions_dict = {}
    equipment_set = set()
    
    for row in raw_results:
        # Set questionnaire title
        questionnaire_data['title'] = row.name
        questionnaire_data['isGeneric'] = row.is_generic  # You may want to add this field to your model
        
        # Group questions by text and order
        question_key = (row.text, row.order, row.type, row.required)
        
        if question_key not in questions_dict:
            questions_dict[question_key] = {
                'text': row.text,
                'type': row.type,
                'required': row.required,
                'order': row.order,
                'options': []
            }
        
        # Add option if it exists
        if row.master_list_id and row.master_list_dim_id and row.dim_value:
            option = {
                'master_list_id': row.master_list_id,
                'master_list_dim_id': row.master_list_dim_id,
                'dim_value': row.dim_value
            }
            # Avoid duplicate options
            if option not in questions_dict[question_key]['options']:
                questions_dict[question_key]['options'].append(option)
        
        # Collect equipment
        if row.equipment_id and row.equipment_name:
            equipment_set.add((row.equipment_id, row.equipment_name))
    
    # Convert questions dict to list, sorted by order
    questions_list = list(questions_dict.values())
    questions_list.sort(key=lambda x: x['order'])
    
    # Remove options key if empty for non-dropdown questions
    for question in questions_list:
        if not question['options']:
            del question['options']
    
    questionnaire_data['questions'] = questions_list
    
    # Add equipment
    questionnaire_data['assignedEquipment'] = [
        {
            'equipment_id': eq_id,
            'equipment_name': eq_name
        }
        for eq_id, eq_name in sorted(equipment_set)
    ]
    print("the questionnaire data is", questionnaire_data)
    
    return questionnaire_data
