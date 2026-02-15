import os
import json
from sqlalchemy import Column, Integer, String,Enum,JSON,insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
import boto3

# ORM Models

Base = declarative_base()

class Supplier(Base):
    __tablename__ = "supplier"
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(255), nullable=False, unique=True)
    role = Column(String(50), nullable=False)
    company_name = Column(String(255))
    company_type = Column(String(100))
    country = Column(String(100))
    first_name = Column(String(100))
    middle_name = Column(String(100))
    last_name = Column(String(100))
    title = Column(String(100))
    designation = Column(String(150))
    department = Column(String(150))
    email_personal = Column(String(255))
    email_company = Column(String(255))
    email_official = Column(String(255))
    address_line1 = Column(String(255))
    address_line2 = Column(String(255))
    address_line3 = Column(String(255))
    state = Column(String(100))
    city = Column(String(100))
    phone_number_b = Column(String(50))
    mobile_phone_official = Column(String(50))
    direct_phone_number = Column(String(50))
    website = Column(String(255))
    e_registered_details = Column(JSON)
    e_registered_verified_status = Column(
        Enum("V", "N", "R", name="verified_status_enum"), default="N"
    ) 
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    ) 


class Advertiser(Base):
    __tablename__ = "advertiser"
    __table_args__ = {"schema": "valuesmart"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_name = Column(String(255), nullable=False, unique=True)
    role = Column(String(50), nullable=False)
    company_name = Column(String(255))
    company_type = Column(String(100))
    country = Column(String(100))
    first_name = Column(String(100))
    middle_name = Column(String(100))
    last_name = Column(String(100))
    title = Column(String(100))
    designation = Column(String(150))
    department = Column(String(150))
    email_personal = Column(String(255))
    email_company = Column(String(255))
    email_official = Column(String(255))
    address_line1 = Column(String(255))
    address_line2 = Column(String(255))
    address_line3 = Column(String(255))
    state = Column(String(100))
    city = Column(String(100))
    phone_number_b = Column(String(50))
    mobile_phone_official = Column(String(50))
    direct_phone_number = Column(String(50))
    website = Column(String(255))
    e_registered_details = Column(JSON)
    e_registered_verified_status = Column(
        Enum("V", "N", "R", name="verified_status_enum"), default="N"
    ) 
    archive = Column(
        Enum('Y', 'N', name='archive_enum'), 
        nullable=False, 
        server_default='N' 
    ) 


class Buyer(Base):
    __tablename__ = "buyer"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)    
    user_name = Column(String(100), nullable=False, unique=True)
    role = Column(String(50), nullable=False)
    email = Column(String(150), nullable=False, unique=True)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)

class Users(Base):
    __tablename__ = "users"
    __table_args__ = {"schema": "valuesmart"}
    id = Column(Integer, primary_key=True, autoincrement=True)    
    user_name = Column(String(100), nullable=False, unique=True)
    entity_id = Column(Integer, nullable=False)
    user_role = Column(
        Enum('BUYER', 'SUPPLIER', 'ADVERTISER', name='user_role_enum'),
        nullable=False,
        server_default='BUYER'
    )

class SupplierApprovedMarket(Base):
    __tablename__ = "supplier_approved_markets"

    id = Column(Integer, primary_key=True)
    supplier_id = Column(Integer, nullable=False)
    unit_operation_id = Column(Integer, nullable=False)
    unit_operation = Column(String(255), nullable=False)
    market_segment_id = Column(Integer, nullable=False)
    market_segment = Column(String(255), nullable=False)
    archive = Column(Enum('Y', 'N'), default='N')


def createDataBaseConnection():
    print("creating database connection")
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

def lambda_handler(event, context):
    SessionLocal=createDataBaseConnection()
    session = SessionLocal() 
    token = event["headers"]
    print("token", token)
    method = event["httpMethod"] 
    methodLower = event["httpMethod"].lower()
    methodPath = event.get("path", "").replace('/', '')
    actions = {
    "post_buyer_registration": post_buyer_registration,
    "post_supplier_registration": post_supplier_registration,
    "post_advertiser_registration": post_advertiser_registration,
    "get_supplier_registration": get_supplier_registration,
    "get_advertiser_registration": get_advertiser_registration,
    "get_user_registration": get_user_registration,
    "get_all_supplier_registration": get_all_supplier_registration,
    "patch_advertiser_registration": patch_advertiser_registration,
    "put_supplier_registration": put_supplier_registration,
    "post_send_email_notification": post_send_email_notification,
    "get_user_id" : get_user_id

    }
    print(f"the method is join {methodLower}_{methodPath}")
    handler = actions.get(f"{methodLower}_{methodPath}")
    if not handler:
        return {"statusCode": 405, "body": "Method not allowed"}
    else:
        return handler(event,session)        


def get_user_id(event, session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        user_name=None
        user_name =queryStringParameters.get("userName")
        role=queryStringParameters.get("role")
        try:
            data =  session.query(Users.id,Users.user_name).filter(Users.user_name == user_name).first()
            print("data", data)
            if not data:
                return send_return_status(404, json.dumps({"error": "no data found"}))
            result = {
               "id": data.id
             }
            print("result", result)
            return send_return_status(200, json.dumps(result))
        except Exception as  e:
            print("error", e)
            return send_return_status(500, {"error": "unable to fetch"})
    except Exception as e:
        print("error", e)
        return send_return_status(500, {"error": str(e)})

def get_all_supplier_registration(event,session):
    try:
        data =  session.query(Supplier.id, Supplier.user_name,Supplier.e_registered_verified_status,Supplier.company_name).all()
        result = [
                {"id": row.id, 
                "userName": row.user_name, 
                "eVerifiedStatus": row.e_registered_verified_status,
                "companyName": row.company_name}
                for row in data
                ]
        return send_return_status(200, json.dumps(result))
    except Exception as  e:
        print("error", e)
        return send_return_status(500, {"error": "unable to fetch"})

def get_user_registration(event,session):
    try:
        queryStringParameters= event.get("queryStringParameters") or {}
        user_id=None    
        user_id =queryStringParameters.get("id")   
        try:

            if user_id:                   
                data =  session.query(Users.id, Users.user_name).filter(Users.id == user_id).first()
                print("data", data )     
                if not data:
                    return send_return_status(404, json.dumps({"error": "no data found"}))
                result = {
                   "id": data.id,
                    "user_name": data.user_name
                  }
                print("result", result ) 
                return send_return_status(200, json.dumps(result))   
            else:    
                data =  session.query(Users.id, Users.user_name).all()
                print("data", data) 
                result = [
                        {"id": row.id, "userName": row.user_name}
                        for row in data
                        ]   
                return send_return_status(200, json.dumps(result))
        except Exception as  e:
            print("error", e)
            return send_return_status(500, {"error": "unable to fetch"})   
    except Exception as e:
        print("error", e)
        return send_return_status(500, {"error": str(e)}) 



def get_supplier_registration(event,session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        supplier_name=None
        supplier_name =queryStringParameters.get("userName")

        
        if supplier_name is None:
                return send_return_status(400, {"error": "supplierName is required"})
        try:         
            data =  session.query(Supplier.id, Supplier.e_registered_details,Supplier.e_registered_verified_status).filter(Supplier.user_name == supplier_name).first()
            print("data", data )     
            if not data:
                return send_return_status(404, json.dumps({"error": "no data found"}))
            result = {
                   "id": data.id,
                    "eRegisteredDetails": data.e_registered_details,
                    "eVerifiedStatus": data.e_registered_verified_status
                  }
            print("result", result )          
            return send_return_status(200, json.dumps(result))
        except Exception as  e:
            print("error", e)
            return send_return_status(500, {"error": "unable to fetch"})   
    except Exception as e:
        print("error", e)
        return send_return_status(500, {"error": str(e)})
    
 

def get_advertiser_registration(event,session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        advertiser_id=None
        advertiser_id =queryStringParameters.get("id")
        
        if advertiser_id is None:
                return send_return_status(400, {"error": "id is required"})   
        data =  session.query(Advertiser.id, Advertiser.user_name,Advertiser.e_registered_details,Advertiser.e_registered_verified_status).filter(Advertiser.id == advertiser_id).first()
        print("data", data )     
        if not data:
            return send_return_status(404, json.dumps({"error": "no data found"}))
        result = {
                   "id": data.id,
                    "user_name": data.user_name,
                    "e_registered_details": data.e_registered_details,
                    "e_registered_verified_status": data.e_registered_verified_status
                  }
        print("result", result )          
        return send_return_status(200, json.dumps(result))
    except Exception as e:
        print("error", e)
        return send_return_status(500, {"error": str(e)})    
    

def post_supplier_registration(event,session):
    body= event.get("body", {}) or {}  
    if not body:
        return send_return_status(400, "Invalid request body")
    required_fields = ["userName"]  
    data= json.loads(body)
    validation_result = check_required_fields(data, required_fields)    

    if validation_result is not True:
            return validation_result

    print("inside post buyer registration")
    try:
        profile = data.get("profile")
        print("profile", profile.get("companyName"))
        supplier = Supplier(
            user_name=data.get("userName"),
            role=data.get("role"),
            company_name=profile.get("companyName"),
            company_type=profile.get("companyType"),
            country=profile.get("country"),
            first_name=profile.get("firstName"),
            middle_name=profile.get("middleName"),
            last_name=profile.get("lastName"),
            title=profile.get("title"),
            designation=profile.get("designation"),
            department=profile.get("department"),
            email_personal=profile.get("email"),
            email_company=profile.get("emailCompany"),
            email_official=profile.get("emailOfficial"),
            address_line1=profile.get("addressLine1"),
            address_line2=profile.get("addressLine2"),
            address_line3=profile.get("addressLine3"),
            state=profile.get("state"),
            city=profile.get("city"),
            phone_number_b=profile.get("phoneNumB"),
            mobile_phone_official=profile.get("mobilePhoneOfficial"),
            direct_phone_number=profile.get("directPhoneNumber"),
            website=profile.get("website"),
            e_registered_details=data
           
    )   
        try:
            SessionLocal=createDataBaseConnection()
            session = SessionLocal()
            session.add(supplier)
            session.commit()  # Commit the transaction
            session.close()
            #status=post_send_email_notification(event)
            status=200
            #if status.get("statusCode") == 200:
            if status == 200:
                return send_return_status(200,json.dumps({"response":"New user registered successfully"}))
            return send_return_status(500, json.dumps({"error": "New user registered successfully unable to send notification"}))    
        except   Exception as e:  
            print("error", e)
            return send_return_status(500, json.dumps({"error": "unable to register","message": str(e)}))
    except Exception as e:
        print("error", e   )
        return send_return_status(500,json.dumps({"error": "unable to register"}))


def put_supplier_registration (event,session):
    try:
        body= event.get("body", {}) or {}  
        if not body:
            return send_return_status(400, "Invalid request body")
        required_fields = ["id"]  
        data= json.loads(body)
        validation_result = check_required_fields(data, required_fields)    

        if validation_result is not True:
            return validation_result

        id =None
        supplierStatus =None
        registeredVetailsstatus =None
        data= json.loads(body)
        if data.get("id"):
            id = data.get("id")
        if data.get("archive"):
            supplierStatus = data.get("archive")
        if data.get("registeredVerifiedStatus"):
            registeredVetailsstatus = data.get("registeredVerifiedStatus")        
        if not supplierStatus and not registeredVetailsstatus:
            return send_return_status(400, json.dumps({"error": "Invalid queryStringParameters"}))   
        record = session.query(Supplier).filter_by(id=id).first()
        if id and supplierStatus:           
            if record:
                record.archive = supplierStatus
                session.commit()
                return send_return_status(200, json.dumps({"response":"archived successfully"}))
            else:
                return send_return_status(500, json.dumps({"response": "no record found "}))
        elif id and registeredVetailsstatus and record:
            supplierApprovedMarkets=None
            supplierApprovedMarkets=data.get("markets")      
            if supplierApprovedMarkets and registeredVetailsstatus == 'V':
                unit_operations = data.get("markets", [])
                values = []
                for unit_op in unit_operations:
                    unit_operation_id = unit_op["unitOperationId"]
                    for ms in unit_op.get("marketSegments", []):
                        market_segment_id = ms["marketSegmentId"]
                        values.append({
                            "supplier_id": id,
                            "unit_operation_id": unit_operation_id,
                            "market_segment_id": market_segment_id
                         
                        })
                stmt = insert(SupplierApprovedMarket).values(values)
                session.execute(stmt)        

            record.e_registered_verified_status = registeredVetailsstatus                    
            # new_user = Users(
            #             user_name=record.user_name,
            #             entity_id=record.id,
            #             user_role='SUPPLIER',     # BUYER | SUPPLIER | ADVERTISER
            #                 )
            # session.add(new_user)          
            session.commit()
                #status=post_send_email_notification(event)
            status=200
                #if status.get("statusCode") == 200:
            if status == 200:
                return send_return_status(200,json.dumps({"response":" user verified status updated successfully"}))
            return send_return_status(500, json.dumps({"error": "user verified status updated successfully unable to send notification"}))    
                   
    except Exception as e:
        return send_return_status(500, json.dumps({"error": str(e)}))


def patch_advertiser_registration(event,session):
    try:
        queryStringParameters= event.get("queryStringParameters")
        if not queryStringParameters:
            return send_return_status(400, json.dumps({"error": "Request queryStringParameters is missing"}))
        id =None
        advetiserStatus =None
        registeredVetailsstatus =None
        for key, value in queryStringParameters.items():
            if key == "id" :
                id = value
            elif key == "advetiserStatus":
                advetiserStatus = value
            elif key == "registeredVerifiedStatus":
                registeredVetailsstatus = value   
            else:
                return send_return_status(400, json.dumps({"error": "Invalid queryStringParameters"}))     
        if id and advetiserStatus:
            record = session.query(Advertiser).filter_by(id=id).first()
            if record:
                record.e_registered_verified_status = advetiserStatus
                session.commit()
                return send_return_status(200, json.dumps({"response":"archived successfully"}))
            else:
                return send_return_status(500, json.dumps({"response": "no record found "}))
                session.close()
                SessionLocal.close()
        elif id and registeredVetailsstatus:
            record = session.query(Advertiser).filter_by(id=id).first()
            if record:
                record.e_registered_verified_status = registeredVetailsstatus
                new_user = Users(
                        user_name=record.user_name,
                        entity_id=record.id,
                        user_role='ADVERTISER',     # BUYER | SUPPLIER | ADVERTISER
                            )
                session.add(new_user)            
                session.commit()
                #status=post_send_email_notification(event)
                status=200
                #if status.get("statusCode") == 200:
                if status == 200:
                    return send_return_status(200,json.dumps({"response":" user verified status updated successfully"}))
                return send_return_status(500, json.dumps({"error": "user verified status updated successfully unable to send notification"}))    
            else:
                return send_return_status(500, json.dumps({"response": "no record found "}))  
    except Exception as e:
        return send_return_status(500, json.dumps({"error": str(e)}))



def post_buyer_registration(event,session)   :
    body= event.get("body", {}) or {}  
    if not body:
        return send_return_status(400, "Invalid request body") 

    required_fields = ["userName"]  
    data= json.loads(body)
    validation_result = check_required_fields(data, required_fields)  
    
    if validation_result is not True:
            return validation_result
    print("inside post buyer registration")
    try:     
        new_user = Buyer(
                        user_name=data.get('userName'),
                        email=data.get('userName'),
                        role='BUYER',     # BUYER | SUPPLIER | ADVERTISER
                        first_name=data.get('firstName'),
                        last_name=data.get('lastName')
                            )
       
        session.add(new_user)                                 
        session.commit()
        return send_return_status(200, json.dumps({"response":"registered successfully"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": "unable to register"}))



def post_advertiser_registration(event,session):
    body= event.get("body", {}) or {}  
    if not body:
        return send_return_status(400, "Invalid request body")

    required_fields = ["userName"]  
    data= json.loads(body)
    validation_result = check_required_fields(data, required_fields)    

    if validation_result is not True:
            return validation_result
    print("inside post buyer registration")
    try:
        profile = data.get("profile",{})
        advertiser = Advertiser(
            user_name=data.get("userName"),
            role=data.get("role"),
            company_name=profile.get("companyName"),
            company_type=profile.get("companyType"),
            country=profile.get("country"),
            first_name=profile.get("firstName"),
            middle_name=profile.get("middleName"),
            last_name=profile.get("lastName"),
            title=profile.get("title"),
            designation=profile.get("designation"),
            department=profile.get("department"),
            email_personal=profile.get("email"),
            email_company=profile.get("emailCompany"),
            email_official=profile.get("emailOfficial"),
            address_line1=profile.get("addressLine1"),
            address_line2=profile.get("addressLine2"),
            address_line3=profile.get("addressLine3"),
            state=profile.get("state"),
            city=profile.get("city"),
            phone_number_b=profile.get("phoneNumB"),
            mobile_phone_official=profile.get("mobilePhoneOfficial"),
            direct_phone_number=profile.get("directPhoneNumber"),
            website=profile.get("website"),
            e_registered_details=data
           
    )   
        try:
            SessionLocal=createDataBaseConnection()
            session = SessionLocal()
            session.add(advertiser)
            session.commit()  # Commit the transaction
            session.close()
            #status=post_send_email_notification(event)
            status=200
            #if status.get("statusCode") == 200:
            if 200 == 200:
                return send_return_status(200,json.dumps({"response":"New user registered successfully"}))
            return send_return_status(500, json.dumps({"error": "New user registered successfully unable to send notification"}))    
        except   Exception as e:  
            print("error", e)
            return send_return_status(500, json.dumps({"error": "unable to register","message": str(e)}))
    except Exception as e:
        print("error", e   )
        return send_return_status(500,json.dumps({"error": "unable to register"}))


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


def check_required_fields(data, required_fields):
    missing = [field for field in required_fields if field not in data or data.get(field) is None]
    if missing:
        return send_return_status(
            400,
            json.dumps({"error": f"Missing required fields: {', '.join(missing)}"})
        )
    else:
        return True

def post_send_email_notification(event):
    try:
        ses = boto3.client('ses', region_name='ap-southeast-2')
        print("ses", ses)
        response = ses.send_email(
        Source='ragini.manjaiah@gmail.com',  # your verified email
        Destination={
            'ToAddresses': ['alwayssflyhigh@gmail.com']
        },
        Message={
            'Subject': {'Data': 'Welcome!'},
            'Body': {
                'Text': {'Data': 'Hello, thanks for joining our platform!'},
                'Html': {'Data': '<h1>Hello!</h1><p>Welcome to our platform.</p>'}
            }
        }
    )
        print("response:", response)
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("response['ResponseMetadata']['HTTPStatusCode']",response['ResponseMetadata']['HTTPStatusCode'])
            return send_return_status(200, 'Email sent successfully!')
        else:
            return send_return_status(500, 'Error sending email: ' + str(response))    
    except Exception as e:
        print("error", e   )
        return send_return_status(500, 'Error sending email: ' + str(e))
