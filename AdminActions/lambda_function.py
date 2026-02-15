import os
import json
import enum
from sqlalchemy import Column, Integer, String,Enum,JSON,Date,update, bindparam,case,text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
import json
import boto3
from datetime import datetime,timezone
from zoneinfo import ZoneInfo
import sys
import logging
from pydantic import ValidationError,BaseModel,Field
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)



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
    uploadstatus= Column(String(10))
    level_id = Column(Integer, nullable=False)
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )

class UnitOperation(Base):
    __tablename__ = "unit_operations"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    unit_operation = Column(String(250))
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

class Variants(Base):
    __tablename__ = "variants"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    dim_id = Column(Integer)
    dim_name = Column(String(50))
    variant = Column(String(100))
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
    dim_name = Column(String(50))
    capacity = Column(String(100))  
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    )
     
    

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
    division_id = Column(Integer)
    division_name = Column(String(250))
    capacity_id = Column(Integer)
    capacity_name = Column(String(250))
   
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

class CalendarTimeRates(Base):
    __tablename__ = "calendar_time_rates"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    booking_date = Column(Date, nullable=False)
    hour = Column(String(5), nullable=False)
    slot_number = Column(Integer, nullable=False)
    rate_points = Column(Integer, nullable=False)

class SupplierWallet(Base):
    __tablename__ = 'supplier_wallet'
    __table_args__ = {'schema': 'valuesmart'}
    id = Column(Integer, primary_key=True, autoincrement=True)
    supplier_id = Column(Integer, nullable=False, unique=True)
    balance = Column(Integer, nullable=False, server_default=text("0"))

class WalletRequest(BaseModel):
    # Field aliases allow you to accept 'entityId' from JSON 
    # but use 'entity_id' in your Python code.
    supplier_id: int = Field(alias="entityId")
    points: int = Field(gt=0) 

    class Config:
        populate_by_name = True


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



s3_client = boto3.client('s3')

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
    print("version",sys.version)

    # Extract some useful info from the JWT
    user_id = claims.get("sub")                 # Cognito's unique user ID (UUID)
    username = claims.get("cognito:username")   # Username in Cognito
    email = claims.get("email")                 # User's email
    groups = claims.get("cognito:groups")       
    SessionLocal=createDataBaseConnection()
    session = SessionLocal()
    methodLower = event["httpMethod"].lower()
    methodPath = event.get("path", "").replace('/', '')
    actions = {
    "get_cancel_policy": get_cancel_policy,
    "patch_cancel_policy": patch_cancel_policy,
    "post_cancel_policy": post_cancel_policy,
    "patch_calendar_time_rates": patch_calendar_time_rates,
    "get_calendar_time_rates": get_calendar_time_rates,
    "post_supplier_wallet": post_supplier_wallet
    }
    print(f"the method is join {methodLower}_{methodPath}")
    handler = actions.get(f"{methodLower}_{methodPath}")

    if handler:
        return handler(event,session)
    else:
        print("check for the other method")    
    
    print("the event is", event)
    method = event["httpMethod"] 
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


def patch_calendar_time_rates(event,session):
    try:
        data = json.loads(event["body"])
        if not isinstance(data, list):
            return send_return_status(400, json.dumps({"error": "Input must be a list"}))   

        for item in data:
            if "bookingDateId" not in item or "ratePoints" not in item:
                return send_return_status(400, json.dumps({"error": "Each item must contain 'bookingDateId' and 'ratePoints'"}))
         
        case_stmt = case(
            {item["bookingDateId"]: item["ratePoints"] for item in data},
            value=CalendarTimeRates.id
                )
        print( "case stmt",case_stmt )        

        stmt = update(CalendarTimeRates).where(
                CalendarTimeRates.id.in_([item["bookingDateId"] for item in data])
                ).values(rate_points=case_stmt)

        result = session.execute(stmt)
        update_records=result.rowcount
        session.commit()
        return send_return_status(200, json.dumps({"message": f"{update_records} Rates updated successfully"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))


    


def get_calendar_time_rates(event,session):
    try:
        data = event.get('queryStringParameters', {}) or {}
        start_date=data.get('startDate')
        end_date=data.get('endDate')

        if not start_date or not end_date:
            return send_return_status(400, json.dumps({"error": "Missing required fields"}))
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            query = (
                session.query(CalendarTimeRates).filter(
               
                    CalendarTimeRates.booking_date >= start_date,
                    CalendarTimeRates.booking_date <= end_date
                    )
                    )
            result = query.all()
            if result:
                response = [
                {
                    "bookingDateId": item.id,
                    "bookingDate": str(item.booking_date),
                    "hour": item.hour,
                    "slotNumber": item.slot_number,
                    "ratePoints": item.rate_points
                }
                for item in result
                ]
                return send_return_status(200, json.dumps(response))
            else:
                return send_return_status(404, json.dumps({"error": "No calendar time rates found"}))
        except Exception as e:
            print(e)
            return send_return_status(500, json.dumps({"error": "unble to fecth tha data"}))    
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))


            
def get_cancel_policy(event,session):
    try:
        query = (
            session.query(CancellationPolicy)
        )
        result = query.all()
        if result:
            response = [
                {
                    "rule_id": item.rule_id,
                    "start_day": item.start_day,
                    "end_day": item.end_day,
                    "deduction_percentage": item.deduction_percentage
                }
                for item in result
            ]
            return send_return_status(200, json.dumps(response))
        else:
            return send_return_status(404, json.dumps({"error": "No cancellation policy found"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))



def post_cancel_policy(event,session):
    try:
        data = json.loads(event.get('body', '{}'))
        start_day = data.get('startDay')
        end_day = data.get('endDay')
        deduction_percentage = data.get('deductionPercentage')
        if not start_day or not end_day or not deduction_percentage:
            return send_return_status(400, json.dumps({"error": "Missing required fields"}))
        try:
            new_policy = CancellationPolicy(
                start_day=start_day,
                end_day=end_day,
                deduction_percentage=deduction_percentage
            )
            session.add(new_policy)
            session.commit()
            return send_return_status(200, json.dumps({"message": "Cancellation policy added successfully"}))
        except Exception as e:
            print(e)
            return send_return_status(500, json.dumps({"error": "Unable to add cancellation policy"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))

def patch_cancel_policy(event,session):
    try:
        data = event.get('queryStringParameters', {}) or {}
        rule_id = data.get('ruleId')
        start_day = data.get('startDay')
        end_day = data.get('endDay')
        deduction_percentage = data.get('deductionPercentage')

        if not rule_id: 
            return send_return_status(400, json.dumps({"error": "Missing required fields"}))
        try:
            policy = session.query(CancellationPolicy).filter_by(rule_id=rule_id).first()
            if not policy:
                return send_return_status(404, json.dumps({"error": "Cancellation policy not found"}))

              # Only update if value is provided
            if start_day is not None:
                policy.start_day = start_day
            if end_day is not None:
                policy.end_day = end_day
            if deduction_percentage is not None:
                policy.deduction_percentage = deduction_percentage
            session.commit()        
            return send_return_status(200, json.dumps({"message": "Cancellation policy updated successfully"}))
        except Exception as e:
            print(e)
            return send_return_status(500, json.dumps({"error": "Unable to update cancellation policy"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))        




def getHandler(event,session):
    try:
        path = event.get("path", "").lower()
        if path.startswith("/equipments_operations_segments"):
            data1 =  session.query(MachineDetails).filter(MachineDetails.archive=='N').all()
            data2 =  session.query(UnitOperation).filter(UnitOperation.archive=='N').all()
            data3 =  session.query(MarketSegment).filter(MarketSegment.archive=='N').all()
            result = []
            responseDict={}
            
            if data1:
                for r in data1:
                    result.append({
                 "id": r.id,
                 "equipment": r.machine_name
               })
            responseDict["equipments"] = result
            result = []
            if data2:
                for r in data2:
                    result.append({
                 "id": r.id,
                 "unit_operation": r.unit_operation
               })
            responseDict["unit_operations"] = result   
            result = []
            if data3:
                for r in data3:
                    result.append({
                 "id": r.id,
                 "market_segment": r.market_segment_name
               }) 
            responseDict["market_segments"] = result 
            result = [] 
            data5=session.query(Variants).filter(Variants.archive=='N').all()
            if data5:
                for r in data5:
                    result.append({
                 "id": r.id,
                 "variant": r.variant
               }) 
            responseDict["variants"] = result
            result = []
            data6=session.query(Capacity).filter(Capacity.archive=='N').all()
            if data6:
                for r in data6:
                    result.append({
                 "id": r.id,
                 "capacity": r.capacity
               }) 
            responseDict["capacities"] = result 
            result = []
            data7=session.query(Division).filter(Division.archive=='N').all()
            if data7:
                for r in data7:
                    result.append({
                 "id": r.id,
                 "division": r.division
               }) 
            responseDict["divisions"] = result   
            print("the response dict is", responseDict)
            
            return send_return_status(200, json.dumps({"response" : responseDict}))

        elif path.startswith("/equipments"):
            # data = session.query(MachineDetails).all()
            machines = (
            session.query(MachineDetails)
            .options(load_only(
                MachineDetails.id, 
                MachineDetails.machine_name, 
                MachineDetails.machine_image_url, 
                MachineDetails.uploadstatus
            ))
            .all()
                )

            result = []
            for r in machines:
                # 3. Local signing (Very fast)
                url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': BUCKET_NAME, 'Key': r.machine_image_url},
                ExpiresIn=3600
                )         
                result.append({
                    "id": r.id,
                    "equipment": r.machine_name,
                    "image_url": url,
                    "uploadstatus": r.uploadstatus
                 })

            return {
                "statusCode": 200,
                "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps(result)
                }
            # result = []
            # for r in data:
            #     result.append({
            #      "id": r.id,
            #      "equipment": r.machine_name,
            #      "image_url": r.machine_image_url,
            #      "uploadstatus":r.uploadstatus
            #    })
            # return {
            #     "statusCode": 200,
            #             "headers": {
            # "Access-Control-Allow-Origin": "*",
            # "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            # "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            #             },
            #     "body": json.dumps(result)

            # }
        elif path.startswith("/unit_operations"):
            data = session.query(UnitOperation).all()
            result = []
            for r in data:
                print("UNIT OPERATION id :", r.dim_id)
                print("UNIT OPERATION name:", r.dim_name)
                result.append({
                 "id": r.id,
                 "unit_operation": r.unit_operation
               })
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            }
        elif path.startswith("/market_segments"):
            data = session.query(MarketSegment).all()
            result = []
            for r in data:
                result.append({
                 "id": r.id,
                 "market_segment": r.market_segment_name
               })
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)


            }   
        elif path.startswith("/variants"):
            data = session.query(Variants).all()
            print("the data is ", data)
            result = []
            for r in data:
                print("VARIANTS:", r)
                result.append({
                 "id": r.id,
                 "variant": r.variant,
                 "archive":r.archive
               })
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            }
        elif path.startswith("/capacity"):
            data = session.query(Capacity).all()
            result = []
            for r in data:
                result.append({
                 "capacity_id": r.id,
                 "capacity_name": r.capacity,
                 "archive":r.archive
               })
            return {
                "statusCode": 200,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                        },
                "body": json.dumps(result)

            }             
    

        
          
    except Exception as e:
        print("the error is ", e)
        return {
            "statusCode": 500,
                        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        },
              
                "body": json.dumps({"error" : "issue in extracting  {e}"})
             }

  


def postHandler(event,session):
    try:
        path = event.get("path", "").lower()
        params = event.get("queryStringParameters", {}) or {}
        body= event.get("body", {}) or {}
        #status = params.get("status", "").lower()

        print("the path is ",path)
        print("the params is",params)
        print("the body is ", body)
        print("the type of body is ", type(body))

        if path.startswith("/equipments"):
            metaDetails=json.loads(body)
            machine_name = metaDetails.get("equipment")
            file_name = metaDetails.get("fileName")
            file_type = metaDetails.get("fileType")
            level_id = metaDetails.get("level_id")
            upload_status=metaDetails.get("uploadstatus")
            dimName="equipments"
            dim_id=get_dimension_id(dimName,session)
            if dim_id is None:
                return send_return_status(500, "internal server error")
            else:    
                machine_image_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{file_name}"
                s3=s3Creation()
                record = MachineDetails(machine_name=machine_name, machine_image_url=machine_image_url,uploadstatus=upload_status,dim_id=dim_id,dim_name=dimName,level_id=level_id)
                try:
                    session.add(record)
                    session.commit()
                except Exception as e:
                    return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for equipments {e}"}))                 
                presigned_url = s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": BUCKET_NAME, "Key": file_name, "ContentType": file_type},
                    ExpiresIn=60
                    ) 
                print("the presigned url is ", presigned_url)
                return send_return_status(200, json.dumps({"uploadUrl": presigned_url }))               
        elif path.startswith("/unit_operations"): 
            metaDetails=json.loads(body)
            unit_operation=metaDetails.get("unit_operations")
            dimName="unit_operations"
            dim_id=get_dimension_id(dimName,session)
            try:
                    if dim_id is None and unit_operation:
                        return send_return_status(500, "internal server error")
                    else:
                        for opt in unit_operation:
                            record = UnitOperation(unit_operation=opt, dim_id=dim_id, dim_name=dimName)
                            session.add(record)
                        session.commit()
                        return send_return_status(200, json.dumps({"response":"uploaded successfully"}))
            except Exception as e:
                print("the error is ", e)
                return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for unit_operations {e}"}))               
                    
        elif path.startswith("/variants"):
            metaDetails=json.loads(body)
            variant=metaDetails.get("variants")
            print("the POST variant is ", variant)
            dimName="variants"
            dim_id=get_dimension_id(dimName,session)
            try:
                if dim_id is None and unit_operation:
                    return send_return_status(500, "internal server error")
                else:        
                    for opt in variant:
                        print("the opt is ", opt)
                        record = Variants(dim_id=dim_id,dim_name=dimName, variant=opt)
                        session.add(record)
                    session.commit()
                    return send_return_status(200,json.dumps({"response":"uploaded successfully"}))
            except Exception as e:
                print("the error is ", e)
                return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for variants {e}"}))               
        
        elif path.startswith("/division"):
            metaDetails=json.loads(body)
            division=metaDetails.get("division")
            print("the POST division is ", division)
            dimName="division"
            dim_id=get_dimension_id(dimName,session)
            print("the dim_id  is ", dim_id)
            try:
                if dim_id is None and division:
                    return send_return_status(500, "internal server error")
                else:        
                    for opt in division:
                        print("the opt is ", opt)
                        record = Division(dim_id=dim_id,dim_name=dimName, division=opt)
                        session.add(record)
                    session.commit()
                    return send_return_status(200,json.dumps({"response":"uploaded successfully"}))
            except Exception as e:
                print("the error is ", e)
                return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for variants {e}"}))               

        elif path.startswith("/capacity"):
            metaDetails=json.loads(body)
            capacity=metaDetails.get("capacity")
            dimName="capacity"
            dim_id=get_dimension_id(dimName,session)
            print("the dim_id is ", dim_id)
            try:
                for opt in capacity:
                    print("the opt is ", opt)
                    record = Capacity(dim_id=dim_id,dim_name=dimName, capacity=opt)
                    session.add(record)
                    session.commit()
                return send_return_status(200, json.dumps({"response":"uploaded successfully"}))    
            except Exception as e:
                print("the error is ", e)
                return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for unit_operations {e}"}))      

        elif path.startswith("/market_segment"): 
            metaDetails=json.loads(body)
            market_segment_name=metaDetails.get("marketSegment")
            dimName="market_segments"
            dim_id=get_dimension_id(dimName,session)
            print("the dim_id is ", dim_id)
            file_name = metaDetails.get("fileName")
            file_type = metaDetails.get("fileType")
            try:
                if dim_id is None:
                    return send_return_status(500, "internal server error")
                else:
                    s3=s3Creation()
                    presigned_url = s3.generate_presigned_url(
                    "put_object",
                    Params={"Bucket": BUCKET_NAME, "Key": file_name, "ContentType": file_type},
                    ExpiresIn=60
                    )
                    record = MarketSegment(market_segment_name=market_segment_name, dim_id=dim_id, dim_name=dimName,image_url=file_name)
                    session.add(record)
                    session.commit()
                    return send_return_status(200, json.dumps({"uploadUrl": presigned_url })) 
            except Exception as e:
                print("the error is ", e)
                return send_return_status(409, json.dumps({"error": f"Duplicate entry/ Integrity issues for market segment {e}"}))                             
        elif path.startswith("/equipment_capabilities_fct"): 
            print("enetered into machine_capabilities_fct")
            metaDetails=json.loads(body)
            #if status == "add": 
            machine = metaDetails.get("equipment")
            unit_ops = metaDetails.get("unit_operation")
            market_segs = metaDetails.get("market_segment")
            variants = metaDetails.get("variant")
            division=metaDetails.get("division")
            capacity = metaDetails.get("capacity")
            
            debug_message = {"response": "form succesfully loaded"}
            records = []
            errorFlag=0
            for c in capacity:
                data=session.query(EquipmentCapabilitiesFct).filter_by(machine_id=machine["id"]).filter_by(unit_operation_id=unit_ops["id"]).filter_by(market_segment_id=market_segs["id"]).filter_by(variant_id=variants["id"]).filter_by(capacity_id=c["id"]).filter_by(division_id=division["id"]).first()                            
                if data is None:
                    record = EquipmentCapabilitiesFct(                                                    
                                machine_id=machine["id"],
                                machine_name=machine["equipment"],
                                unit_operation_id=unit_ops["id"],
                                unit_operation =unit_ops["unit_operation"],
                                market_segment_id=market_segs["id"],
                                market_segment_name=market_segs["market_segment"],
                                variant_id=variants["id"],
                                variant_name=variants["variant"],
                                division_id=division["id"],
                                division_name=division["division"],
                                capacity_id=c["id"],
                                capacity_name=c["capacity"]
                                )
                    records.append(record)
                else:
                    response_message = {"response": "data already exists"}

                
            try:
                session.add_all(records)
                session.commit()
                return {
                      "statusCode": 200,
                      "headers": {
                      "Access-Control-Allow-Origin": "*",
                      "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                      "Access-Control-Allow-Methods": "OPTIONS,GET,POST"},
                      "body": json.dumps(debug_message)
                      }    
            except Exception as e:            
                errorFlag=1
                errorMessage=f"unable to insert into machine_capabilities_fct due to {e}"
                return {
                      "statusCode": 500,
                      "headers": {
                      "Access-Control-Allow-Origin": "*",
                      "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                      "Access-Control-Allow-Methods": "OPTIONS,GET,POST"},
                      "body": json.dumps({"response": errorMessage})
                      }
              
        elif path.startswith("/equipment_capabilities_search")  :
            filters = []
            result=[]
            responseDict={}
            metaDetails = json.loads(event.get("body", "{}")) 
            print("the metaDetails params are ", metaDetails)
            machine = metaDetails.get("equipment") or {} 
            print("the machine params are ", machine)
            unit_operation = metaDetails.get("unit_operation") or {}
            print("the unit_operation params are ", unit_operation)
            market_segment = metaDetails.get("market_segment") or {}
            print("the market_segment params are ", market_segment)
            variant = metaDetails.get("variant") or {}
            print("the variant params are ", variant)
            capacity = metaDetails.get("capacity") or {}
            print("the capacity params are ", capacity)
            division = metaDetails.get("division") or {}
            print("the division params are ", division)
            if  machine:
                filters.append(EquipmentCapabilitiesFct.machine_id == machine["id"])
                filters.append(EquipmentCapabilitiesFct.machine_name == machine["equipment"])
            if unit_operation:
                filters.append(EquipmentCapabilitiesFct.unit_operation_id == unit_operation["id"])
                filters.append(EquipmentCapabilitiesFct.unit_operation == unit_operation["unit_operation"])
            if market_segment:
                filters.append(EquipmentCapabilitiesFct.market_segment_id == market_segment["id"])
                filters.append(EquipmentCapabilitiesFct.market_segment_name == market_segment["market_segment"])
            if variant:
                filters.append(EquipmentCapabilitiesFct.variant_id == variant["id"])
                filters.append(EquipmentCapabilitiesFct.variant_name == variant["variant"])    
            if division:
                filters.append(EquipmentCapabilitiesFct.division_id == division["id"])
                filters.append(EquipmentCapabilitiesFct.division_name == division["division"])
            if capacity:
                filters.append(EquipmentCapabilitiesFct.capacity_id == capacity["id"])
                filters.append(EquipmentCapabilitiesFct.capacity_name == capacity["capacity"])
            print("the filters are ", filters)
            #query = session.query(MarketCapabilities)
            query = (
                  session.query(EquipmentCapabilitiesFct, MachineDetails).join(MachineDetails, EquipmentCapabilitiesFct.machine_id == MachineDetails.id)
                 )
            if filters:  
                query = query.filter(and_(*filters))
                print("the query formed is :",query )
            results = query.all()
            if results:
                for i,j in results:
                    result.append({"id":i.id,
                                "equipment_id":i.machine_id,
                                "equipment":i.machine_name,
                                "unit_operation_id":i.unit_operation_id,
                                "unit_operation":i.unit_operation,
                                "market_segment_id":i.market_segment_id,
                                "market_segment":i.market_segment_name,
                                "variant_id":i.variant_id,
                                "variant":i.variant_name,
                                "division_id":i.division_id,
                                "division":i.division_name,
                                "capacity_id":i.capacity_id,
                                "capacity":i.capacity_name,
                                "image_url": s3_client.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': j.machine_image_url})
                                })
                print("the result is ", result) 
                responseDict["equipment_capabilities_fct"] = result
                return send_return_status(200, json.dumps({"response" : responseDict}))

            else:
                return send_return_status(200, json.dumps({"response" : "no data found"}))

        elif path.startswith("/supplier_eregistration"):
            supplier=supplier_signup_json(json.loads(body))
            session.add(supplier)
            session.commit()
            return send_return_status(200, json.dumps({"response":"uploaded successfully"}))

       
    except SQLAlchemyError as e:
        print("the error is ", e)
        return send_return_status(500, json.dumps({"error": f"Internal Server Error {e}"}))
    finally:
        session.close()





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
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        }, "body": json.dumps({"error": str(e)})}



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

def supplier_signup_json(data: dict) -> Supplier:
    profile = data.get("profile", {})
    return Supplier(
        user_name=data.get("userName"),
        role="supplier",
        company_name=profile.get("companyName"),
        country=profile.get("country"),
        company_address="; ".join(
            filter(None, [profile.get("addressLine1"), profile.get("addressLine2"), profile.get("addressLine3")])
        ),
        company_number=profile.get("phoneNumB"),
        company_website=profile.get("website"),
        contact_title=profile.get("title"),
        contact_first_name=profile.get("firstName"),
        contact_middle_name=profile.get("middleName"),
        contact_last_name=profile.get("lastName"),
        contact_designation=profile.get("designation"),
        contact_department=profile.get("department"),
        contact_official_mobile_number=profile.get("mobilePhoneOfficial"),
        contact_direct_mobile_number=profile.get("directPhoneNumber"),
        contact_official_email_id=profile.get("emailOfficial"),
        state=profile.get("state"),
        city=profile.get("city"),
        pincode=profile.get("pincode"),
        e_registered_details=data

    )    



def post_supplier_wallet(event, session):
    try:
        # 1. Parse and Validate
        body = event.get("body")
        if not body:
            return send_return_status(400, json.dumps({"error": "Missing request body"}))
        
        raw_data = json.loads(body) if isinstance(body, str) else body
        # Pydantic validates the existence of entityId and points here
        data = WalletRequest(**raw_data)

        # 2. Database Transaction with Locking
        # .with_for_update() is vital for credit/debit consistency
        wallet = (
            session.query(SupplierWallet)
            .filter_by(supplier_id=data.supplier_id)
            .with_for_update()
            .first()
        )
        if wallet:
            wallet.balance += data.points
        else:
            # Creation path: only storing the ID and balance
            wallet = SupplierWallet(
                supplier_id=data.supplier_id,
                balance=data.points
            )
            session.add(wallet)
        # 3. Create Audit Trail
        tx = SupplierTransaction(
            supplier_id=data.supplier_id,
            credit_debit="CREDIT",
            amount=data.points
        )
        session.add(tx)
        # 4. Atomic Commit
        session.commit()
        return send_return_status(200, json.dumps({
            "supplier_id": data.supplier_id,
            "new_balance": wallet.balance
        }))
    except ValidationError as e:
        # Returns a clear 400 error if entityId or points are missing/invalid
        return send_return_status(400, e.json())
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"DB Error: {str(e)}")
        return send_return_status(500, json.dumps({"error": "Database error"}))
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected Error: {str(e)}")
        return send_return_status(500, json.dumps({"error": "Internal server error"}))

