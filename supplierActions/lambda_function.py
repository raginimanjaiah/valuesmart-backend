import os
import json
from sqlalchemy import Column, Integer, String,JSON,Enum,select,func,Boolean,DateTime,text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
import json
import boto3
import os
from pydantic import BaseModel, conlist
from sqlalchemy import select, func
from datetime import datetime, timedelta,timezone
import enum
import logging
from zoneinfo import ZoneInfo


Base = declarative_base()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ORM Models
class Equipments(Base):
    __tablename__ = "equipments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_name = Column(String(250))
    machine_image_url = Column(String(500))
    level_id = Column(Integer)
    uploadstatus= Column(String(10))


class UnitOperations(Base):
    __tablename__ = "unit_operations"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    unit_operation = Column(String(250))
    

class MarketSegments(Base):
    __tablename__ = "market_segments"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    market_segment_name = Column(String(250))

class Variants(Base):
    __tablename__ = "variants"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    variant = Column(String(250))    

class Capacity(Base):
    __tablename__ = "capacity"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    capacity = Column(String(250))       
    

class EquipmentCapabilitiesFct(Base):
    __tablename__ = "equipment_capabilities_fct"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_id = Column(Integer)
    machine_name = Column(String(250))
    unit_operation_id = Column(Integer)
    unit_operation = Column(String(50))
    market_segment_id = Column(String(50))
    market_segment_name = Column(String(50))
    variant_id = Column(Integer) 
    variant_name = Column(String(250))
    capacity_id = Column(Integer)
    capacity_name = Column(String(250))
   

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

class QuestionnaireEquipmentMap(Base):
    __tablename__ = "questionnaire_Equipment_map"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    questionnaire_id = Column(Integer, nullable=False)
    machine_id = Column(Integer, nullable=False)

class SupplierApprovedMarkets(Base):
    __tablename__ = "supplier_approved_markets"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(Integer, nullable=False)
    unit_operation_id = Column(Integer, nullable=False)
    unit_operation = Column(String(255), nullable=False)
    market_segment_id = Column(Integer, nullable=False)
    market_segment = Column(String(255), nullable=False)
    archive = Column(Enum('Y', 'N'), default='N')  

class LevelPoints(Base):
    __tablename__ = "level_points"
    __table_args__ = {"schema": "valuesmart"}  # replace with DB_NAME if needed
    id = Column(Integer, primary_key=True, autoincrement=True)
    level = Column(String(100))
    points = Column(Integer)
    archive = Column(Enum('Y', 'N'), default='N')


class EnquirySupplierMatch(Base):
    __tablename__ = 'enquiry_supplier_matches'
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    enquiry_id = Column(Integer, nullable=False)
    supplier_id = Column(Integer, nullable=False)
    notified = Column(Boolean, default=False)
    viewed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

class BuyerEnquiredEquipment(Base):
    __tablename__ = 'buyer_enquired_equipments'
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    buyer_id = Column(Integer, nullable=False)
    market_segment_id = Column(Integer, nullable=False)
    unit_operation_id = Column(Integer, nullable=False)
    equipment_id = Column(Integer, nullable=False)
    capacity_id = Column(Integer, nullable=True)
    e_registered_details = Column(JSON, nullable=True)
    archive = Column(Enum('Y', 'N'), server_default='N')

class SupplierWallet(Base):
    __tablename__ = 'supplier_wallet'
    __table_args__ = {'schema': 'valuesmart'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(Integer, nullable=False, unique=True)
    balance = Column(Integer, nullable=False, server_default=text("0"))    

# 1. Define Python Enum for Type Safety
class TransactionType(enum.Enum):
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"

class SupplierTransaction(Base):
    __tablename__ = 'supplier_transactions'
    __table_args__ = {'schema': 'valuesmart'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(Integer, nullable=False)
    credit_debit = Column(Enum(TransactionType), nullable=False)
    amount = Column(Integer, nullable=False)


# DB connection
# DB_USER = os.environ["DB_USER"]
# DB_PASS = os.environ["DB_PASS"]
# DB_HOST = os.environ["DB_HOST"]
# DB_NAME = os.environ["DB_NAME"]



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
    method = event["httpMethod"] 
    methodLower = event["httpMethod"].lower()
    methodPath = event.get("path", "").replace('/', '')
    actions = {
    "get_supplier_approved_markets": get_supplier_approved_markets,
    "post_supplier_registered_equipments": post_supplier_registered_equipments,
    "get_buyer_enquiry_list": get_buyer_enquiry_list,
    "get_enquire_count": get_enquire_count,
    "post_download_enquiry": post_download_enquiry,
    "get_supplier_wallet": get_supplier_wallet,
    

    }
    print(f"the method is join {methodLower}_{methodPath}")
    handler = actions.get(f"{methodLower}_{methodPath}")
    # if not handler:
    #     return {"statusCode": 405, "body": "Method not allowed"}
    if handler:
        return handler(event,session)      
            



def get_supplier_approved_markets(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        supplier_id=None
        supplier_id =queryStringParameters.get("supplierId")
        if supplier_id is None:
            return send_return_status(400, {"error": "id is required"})

# Subquery: one questionnaire per machine_id
        questionnaire_subq = (
            select(
                QuestionnaireEquipmentMap.machine_id.label("machine_id"),
                func.max(QuestionnaireEquipmentMap.questionnaire_id).label("questionnaire_id")
                )
            .group_by(QuestionnaireEquipmentMap.machine_id)
            .subquery()
                     )

# Main statement
        stmt = (
            select(
                SupplierApprovedMarkets.supplier_id,
                SupplierApprovedMarkets.market_segment_id,
                MarketSegments.market_segment_name,
                SupplierApprovedMarkets.unit_operation_id,
                UnitOperations.unit_operation,
                EquipmentCapabilitiesFct.machine_id,          # <-- machine_id here
                Equipments.machine_name,
                Equipments.machine_image_url,
                questionnaire_subq.c.questionnaire_id
                )
            .join(
                MarketSegments,
                SupplierApprovedMarkets.market_segment_id == MarketSegments.id
                )
            .join(
                UnitOperations,
                SupplierApprovedMarkets.unit_operation_id == UnitOperations.id
                )
            .join(
                EquipmentCapabilitiesFct,
                (SupplierApprovedMarkets.market_segment_id == EquipmentCapabilitiesFct.market_segment_id) &
                (SupplierApprovedMarkets.unit_operation_id == EquipmentCapabilitiesFct.unit_operation_id)
                )
            .join(
                Equipments,
                EquipmentCapabilitiesFct.machine_id == Equipments.id
                )
            .join(
                questionnaire_subq,
                Equipments.id == questionnaire_subq.c.machine_id   # <-- join on machine_id
             )
            .where(SupplierApprovedMarkets.supplier_id == supplier_id,
                SupplierApprovedMarkets.archive == "N")
            )

        results = session.execute(stmt).mappings().all()
        json_results = [dict(row) for row in results]


        if  not results:
            return send_return_status(404, json.dumps({"error": "No data found"}))
          
        return send_return_status(200, json.dumps(json_results))    
    except SQLAlchemyError as e:
        return send_return_status(500, json.dumps({"error": str(e)}))



def post_supplier_registered_equipments(event, session):
    try:
        body = event.get("body")
        supplier_id=None
        market_segment_id=None
        unit_operation_id=None
        equipment_id=None
        capacity_id=None
        if not body:
            return send_return_status(400, json.dumps({"error": "Request body is missing"}))
        data = json.loads(body)
        print("data", data )
        supplier_id = data.get("supplierId")
        market_segment_id = data.get("marketSegmentId")
        unit_operation_id = data.get("unitOperationId")
        equipment_id = data.get("equipmentId")
        answers = data.get("answers",[])
        e_registered_details = data

        if not all([supplier_id, market_segment_id, unit_operation_id, equipment_id]):      
            return send_return_status(400, json.dumps({"error": "Missing required fields"}))

        for item in answers:
            if item['question_text'] == "capacity":
                capacities = item.get("answer" ,[])
                for i in capacities:
                    capacity_id = i
                    new_record = SupplierRegisteredEquipments(
                        supplier_id=supplier_id,
                        market_segment_id=market_segment_id,
                        unit_operation_id=unit_operation_id,
                        equipment_id=equipment_id,
                        capacity_id=capacity_id,
                        e_registered_details=e_registered_details
                        )   
                    session.add(new_record)    
        if capacity_id is None:
            return send_return_status(400, json.dumps({"error": "Missing required fields"}))

        session.commit()
        return send_return_status(200, json.dumps({"message": "Record created successfully"}))
    except SQLAlchemyError as e:
        return send_return_status(500, json.dumps({"error": str(e)}))

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


def get_buyer_enquiry_list(event, session):
    # It's good practice to extract parameters safely
    query_params = event.get('queryStringParameters') or {}
    supplier_id = query_params.get('supplier_id')
    if not supplier_id:
        return send_return_status(400, json.dumps({"error": "Missing supplier_id in query or path"}))
    try:
        # 1. Calculate the cutoff date
        cutoff_date = datetime.utcnow() - timedelta(days=180)

        # 2. Execute Query
        results = (
            session.query( EnquirySupplierMatch.viewed, EnquirySupplierMatch.enquiry_id)
            .filter(
                and_(
                    EnquirySupplierMatch.supplier_id == supplier_id,
                    EnquirySupplierMatch.viewed == 0,
                    EnquirySupplierMatch.created_at >= cutoff_date 
                )
            )
            .all()
        )

        # 3. Format results
        enquiries_list = [{'id': r.enquiry_id, 'viewed': r.viewed} for r in results]

        return send_return_status(200, json.dumps({
            'supplier_id': supplier_id,
            'enquiries': enquiries_list,
            'count': len(enquiries_list)
        }))

    except SQLAlchemyError as e:
        # Log the actual technical error for debugging
        logger.error(f"Database error for supplier {supplier_id}: {str(e)}")
        # Return a generic error to the user (don't leak DB internals)
        return send_return_status(500, json.dumps({"error": "Internal database error"}))
    
    except Exception as e:
        # Catch-all for logic errors (e.g. date calculation issues)
        logger.error(f"Unexpected error: {str(e)}")
        return send_return_status(500, json.dumps({"error": "An unexpected error occurred"}))


def get_enquire_count(event, session):
    try:
        query_params = event.get('queryStringParameters') or {}
        supplier_id = query_params.get('supplier_id')
        if not supplier_id:
            return send_return_status(400, json.dumps({"error": "Missing supplier_id in query or path"}))
    
        # 2. Optimized query using your index
        # We use .scalar() to get the integer directly
        unread_count = (session.query(func.count(EnquirySupplierMatch.id))
            .filter(
            EnquirySupplierMatch.supplier_id == supplier_id,
            EnquirySupplierMatch.viewed == 0
            ).scalar())

        logger.info(f"Supplier {supplier_id} has {unread_count} unread enquiries.")
        return send_return_status(200, json.dumps({
                'supplier_id': supplier_id,
                'unread_count': unread_count or 0
            }))
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return send_return_status(500, json.dumps({'message': 'Internal Server Error'}))





class BulkDownloadRequest(BaseModel):
    supplier_id: int
    enquiry_ids: list[int] # Expects [101, 102, 103]

def post_download_enquiry(event, session):
    try:
        # API Gateway sends the payload as a string in 'body'
        body_data = json.loads(event.get("body", "{}"))
        
        # Now unpack the parsed dictionary
        data = BulkDownloadRequest(**body_data)
    except Exception as e:
        return {"statusCode": 400, "body": f"Validation Error: {str(e)}"}
    try:
        # 1. Filter out already paid enquiries (Idempotency)
        already_paid = session.query(EnquirySupplierMatch.enquiry_id).filter(
            EnquirySupplierMatch.supplier_id == data.supplier_id,
            EnquirySupplierMatch.enquiry_id.in_(data.enquiry_ids),
            EnquirySupplierMatch.viewed == 1
        ).all()
        
        already_paid_ids = [r[0] for r in already_paid]
        ids_to_charge = [eid for eid in data.enquiry_ids if eid not in already_paid_ids]

        # 2. FETCH ENQUIRY DETAILS (Instead of just a sum)
        # We join everything to get the points AND the actual data in one go
        enquiry_details = (
            session.query(
                BuyerEnquiredEquipment.e_registered_details.label("enquiry_details"), 
                BuyerEnquiredEquipment.id,
                Equipments.machine_name.label("equipment_name"),
                LevelPoints.points
            )
            .join(Equipments, Equipments.id == BuyerEnquiredEquipment.equipment_id)
            .join(LevelPoints, LevelPoints.id == Equipments.level_id)
            .filter(BuyerEnquiredEquipment.id.in_(ids_to_charge))
            .all()
        )

        # 3. Calculate Total Cost from the fetched details
        total_cost = sum(item.points for item in enquiry_details)

        # 4. Lock Wallet and Validate Balance
        wallet = session.query(SupplierWallet).filter_by(supplier_id=data.supplier_id).with_for_update().first()
        
        if not wallet or wallet.balance < total_cost:
            return {"statusCode": 402, "body": f"Need {total_cost} points, but only have {wallet.balance}"}

        # --- EXECUTE TRANSACTION ---
        wallet.balance -= total_cost

        new_log = SupplierTransaction(
            supplier_id=data.supplier_id,
            credit_debit='DEBIT',
            amount=total_cost,
            created_by=str(data.supplier_id),
            remarks=f"Bulk unlock of {len(ids_to_charge)} enquiries"
        )
        session.add(new_log)

        session.query(EnquirySupplierMatch).filter(
            EnquirySupplierMatch.supplier_id == data.supplier_id,
            EnquirySupplierMatch.enquiry_id.in_(ids_to_charge)
        ).update({"viewed": 1}, synchronize_session=False)

        session.commit()

        # 5. FORMAT DATA FOR THE SUPPLIER
        result_data = []
        for enquiry in enquiry_details:
            result_data.append({
                "enquiry_details": enquiry.enquiry_details,
                "enquiry_id": enquiry.id,
                "equipment_name": enquiry.equipment_name,
                "points_deducted": cost
            })
        send_return_status(200, json.dumps(result_data))

    except Exception as e:
        session.rollback()
        return {"statusCode": 500, "body": f"Bulk transaction failed: {str(e)}"}
    finally:
        session.close()


def get_supplier_wallet(event,session):
    params = event.get('queryStringParameters', {}) or {}
    supplier_id = params.get('supplier_id')

    if not supplier_id:
        return send_return_status(400, json.dumps({"error": "Missing required query parameter: supplier"}))
    try:
        query = (
            session.query(SupplierWallet)
            .filter(SupplierWallet.supplier_id == supplier_id)
        )
        result = query.first()
        if result:
            response = {
                "supplier_id": result.supplier_id,
                "balance": result.balance
            }
            return send_return_status(200, json.dumps(response))
        else:
            return send_return_status(404, json.dumps({"error": "supplier wallet not found"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))

