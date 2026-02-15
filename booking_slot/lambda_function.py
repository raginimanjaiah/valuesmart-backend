import json
from sqlalchemy import Column, Integer, String,Text, Boolean, ForeignKey, text,JSON,Enum,select,Date, DateTime,DECIMAL,case,BigInteger,insert,update
from sqlalchemy.orm import declarative_base, sessionmaker, aliased
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_
from sqlalchemy import func
import json
import boto3
import os
import uuid
from datetime import datetime,timezone
import random
import time
from zoneinfo import ZoneInfo
from sqlalchemy import text



Base = declarative_base()    
class CalendarTimeRates(Base):
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
    booking_id = Column(String(100), nullable=False)
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(100)),
    division_id = Column(Integer, nullable=False)
    booking_date_id = Column(Integer, nullable=False)
    booking_points = Column(Integer, nullable=False)
    booking_status = Column(
        Enum("o", "w", "c", "ex", name="booking_status_enum"),
        nullable=False
    )
    waiting_position = Column(Integer, nullable=True)
    url = Column(String(500))
    upload_status = Column(String(10))
    approval_status = Column(
        Enum("pending", "approved", "rejected", name="approval_status_enum"),
        nullable=False
    )
    updated_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    ) 



class AdvertiserWallet(Base):
    __tablename__ = "advertiser_wallets"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)
    entity_name = Column(String(100), nullable=False)
    balance = Column(BigInteger, nullable=False, server_default=text("0"))

class AdvertiserWalletTransaction(Base):
    __tablename__ = "advertiser_transactions"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    entity_id = Column(BigInteger, nullable=False)
    credit_debit = Column(String(10), nullable=False)  # CREDIT / DEBIT
    amount = Column(BigInteger, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

class CancellationPolicy(Base):
    __tablename__ = 'cancellation_policy'

    rule_id = Column(Integer, primary_key=True, autoincrement=True)
    start_day = Column(Integer, nullable=False)   # maximum days before AD date
    end_day = Column(Integer, nullable=False)     # minimum days before AD date
    deduction_percentage = Column(Integer, nullable=False)


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
        return None




def lambda_handler(event, context):
    token = event["headers"]
    print(f"the event headers  is {token}")
    SessionLocal=createDataBaseConnection()
    session = SessionLocal()
    actions = {
    "get_slot" : get_slot,
    "post_slot" : post_slot,
    "post_ad_wallet": post_ad_wallet,
    "patch_slot" : patch_slot,
    "get_ad_wallet": get_ad_wallet,
    "get_user_slot": get_user_slot,
    "delete_slot" : delete_slot,
    "post_refund_slot": post_refund_slot
    }
    methodLower = event["httpMethod"].lower()
    methodPath = event.get("path", "").replace('/', '')
    print(f"the method is join {methodLower}_{methodPath}")
    handler = actions.get(f"{methodLower}_{methodPath}")
    if handler:
        return handler(event,session)
    else:
        print("check for the other method")



def post_refund_slot(event,session):
    try:
        with session.begin():  # ensures commit/rollback
            result = session.execute(
                text("""
                    CALL refund_slot()
                """)
                
            )
            results_dict = [dict(row._mapping) for row in result.fetchall()]

        return send_return_status(201, json.dumps(results_dict))

    except SQLAlchemyError as e:
        session.rollback()
        print("Database error:", e)
        return send_return_status(500, json.dumps({"error": str(e)}))

    except Exception as e:
        session.rollback()
        print("Unexpected error:", e)
        return send_return_status(500, json.dumps({"error": str(e)}))




def patch_slot(event,session):
    try:
        params = event.get('queryStringParameters', {}) or {}
        entity_id = params.get('id')
        booking_date_id=params.get('bookingDateId')
        url=params.get('url')
        upload_status=params.get('uploadStatus')

        if not entity_id or not booking_date_id or not url :
            return send_return_status(400, json.dumps({"error": "Missing required query parameter"}))

        try:
            stmt = (
                update(SlotBookingRequest)
                .where(
                SlotBookingRequest.entity_id == entity_id,
                SlotBookingRequest.booking_date_id == booking_date_id
                )
                .values(url=url,upload_status=upload_status)
                )

            result = session.execute(stmt)
            session.commit()

            if result.rowcount == 0:
                return send_return_status(404, json.dumps({"error": "Slot not found for this user"}))

            return send_return_status(200, json.dumps({"message": "Slot updated successfully"}))     
        except Exception as e:
            print(e)
            return send_return_status(500, json.dumps({"error": "unable to update url the slot"}))         
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))

def get_ad_wallet(event,session):
    params = event.get('queryStringParameters', {}) or {}
    entity_id = params.get('id')

    if not entity_id:
        return send_return_status(400, json.dumps({"error": "Missing required query parameter: entityId"}))

    try:
        entity_id = entity_id
        query = (
            session.query(AdvertiserWallet)
            .filter(AdvertiserWallet.entity_id == entity_id)
        )
        result = query.first()
        if result:
            response = {
                "entity_id": result.entity_id,
                "entity_name": result.entity_name,
                "balance": result.balance
            }
            return send_return_status(200, json.dumps(response))
        else:
            return send_return_status(404, json.dumps({"error": "Advertiser wallet not found"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))


def post_ad_wallet(event,session):
    try:
        data = event.get("body")
        if not data:
            return send_return_status(400, json.dumps({"error": "Request body is missing"}))       
        required_fields = ["entityId","entityName", "points"]   
        metaDetails=json.loads(data)
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result     
        entity_id = metaDetails.get("entityId")
        entity_name=metaDetails.get("entityName")
        curr_balance = metaDetails.get("points")

        wallet = session.query(AdvertiserWallet).filter_by(entity_id=entity_id).first()

        if wallet:
            wallet.balance += curr_balance
        else:
            wallet = AdvertiserWallet(
        entity_id=entity_id,
        entity_name=entity_name,
        balance=curr_balance
    )
        session.add(wallet)

# insert transaction record
        tx = AdvertiserWalletTransaction(
            entity_id=entity_id,
            credit_debit="CREDIT",
            amount=curr_balance
         )
        session.add(tx)

        session.commit()
        # session.add(new_wallet)
        # session.commit()
        return send_return_status(200, json.dumps({"message": "Advertiser wallet created successfully"}))
    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))

def get_slot(event,session):
    params = event.get('queryStringParameters', {}) or {}
    booking_date=None
    local_timezone=None
    booking_date = params.get('bookingDate')
    local_timezone= params.get("localTz")

    if  booking_date is None and local_timezone is None:
        return send_return_status(400, json.dumps({"error": "Missing required query parameter: booking_date"}))     

    try:
        booking_date_obj = datetime.strptime(booking_date, "%Y-%m-%d").date()
        print("the booking date is", booking_date_obj)
        query = (
        session.query(
        func.date(
        func.convert_tz(
            func.concat(CalendarTimeRates.booking_date, " ", CalendarTimeRates.hour),
            'UTC',
            local_timezone
        )
        ).label("local_date"),
        func.time_format(
        func.time(
            func.convert_tz(
                func.concat(CalendarTimeRates.booking_date, " ", CalendarTimeRates.hour),
                'UTC',
                local_timezone
            )
        ),
        '%H:%i'
       ).label("local_hour")  ,  
        CalendarTimeRates.booking_date,
        CalendarTimeRates.id,
        CalendarTimeRates.hour,
        CalendarTimeRates.slot_number,
        CalendarTimeRates.rate_points,
        SlotBookingRequest.booking_id,
        SlotBookingRequest.booking_points,
        func.max(   case(
                (SlotBookingRequest.booking_status == "o", 
                 SlotBookingRequest.entity_id),
                else_=None
            )).label("confirmed_entity_id"),

        func.group_concat(
            case(
                (SlotBookingRequest.booking_status == "w",
                 func.concat(
                     SlotBookingRequest.waiting_position,
                     ":",
                     SlotBookingRequest.entity_id
                 )),
                else_=None
            )
            ).label("waiting_list")
    )
    .outerjoin(
        SlotBookingRequest,
        SlotBookingRequest.booking_date_id == CalendarTimeRates.id)
    .filter(CalendarTimeRates.booking_date == booking_date_obj)
    .group_by(CalendarTimeRates.id)
            )
        results = query.all()
        print("the result is", results)

        response = [
    {
        "bookingDateId": row.id,
        "localDate": str(row.local_date),
        "localHour": str(row.local_hour),
        "calenderDate": str(row.booking_date)
         ,"hour": str(row.hour)
         ,"slotNumber": int(row.slot_number)
         ,"calenderRatePoints": int(row.rate_points)
         ,"bookingId": row.booking_id if row.booking_id else None
         ,"bookingPoints": int(row.booking_points) if row.booking_points else None
         ,"confirmed": int(row.confirmed_entity_id) if row.confirmed_entity_id else None
         ,"waitingList": [
            {
            "waitingPosition": int(item.split(":")[0]),
            "entityId": int(item.split(":")[1])
           }
           for item in row.waiting_list.split(",")
            if ":" in item
           ] if row.waiting_list else []
      }
          for row in results
        ]
        return send_return_status(200, json.dumps(response))
    except Exception as e:
        print(e)
        return send_return_status(500, {"error": str(e)})

    


def get_user_slot(event,session):
    params = event.get('queryStringParameters', {}) or {}
    entity_id = params.get('id')
    local_timezone= params.get("localTz")

    if not entity_id or not local_timezone:
        return send_return_status(400, json.dumps({"error": "Missing required query parameter"}))

    utc_today = utc_today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
  
    print("the utc today is", utc_today)

    try:


        result = (
        session.query(
        func.date(
            func.convert_tz(
                func.concat(CalendarTimeRates.booking_date, " ", CalendarTimeRates.hour),
                'UTC',
                local_timezone
            )
        ).label("local_date"),

        func.time_format(
            func.time(
                func.convert_tz(
                    func.concat(CalendarTimeRates.booking_date, " ", CalendarTimeRates.hour),
                    'UTC',
                    local_timezone
                )
            ),
            '%H:%i'
        ).label("local_hour"),

        SlotBookingRequest.entity_id,
        SlotBookingRequest.division_id,
        Division.division.label("division_name"),

        CalendarTimeRates.booking_date,
        CalendarTimeRates.hour,

        SlotBookingRequest.booking_date_id,
        SlotBookingRequest.booking_status,
        SlotBookingRequest.waiting_position,
        SlotBookingRequest.booking_id,
        SlotBookingRequest.booking_points,
        SlotBookingRequest.url,
        SlotBookingRequest.updated_at,

        func.row_number().over(
            partition_by=[
                SlotBookingRequest.booking_date_id,
                SlotBookingRequest.entity_id,
                SlotBookingRequest.division_id
            ],
            order_by=SlotBookingRequest.updated_at.desc()
        ).label("rn")
    )
    .join(
        CalendarTimeRates,
        CalendarTimeRates.id == SlotBookingRequest.booking_date_id
    )
    .join(
        Division,
        Division.id == SlotBookingRequest.division_id
    )
    .filter(
        CalendarTimeRates.booking_date > utc_today,
        SlotBookingRequest.entity_id == entity_id,
        Division.archive == 'N'
    )
    .subquery()
)

        final_result = (
        session.query(result)
        .filter(result.c.rn == 1)
        .all()
)

        if not final_result:
            data=[]
            return send_return_status(200,json.dumps(data))

        data = [
        {
        "id": row.entity_id,
        "bookingDateId": row.booking_date_id,
        "bookingStatus": row.booking_status,
        "waitingPosition": row.waiting_position,
        "url": row.url,
        "localDate": row.local_date.isoformat() if row.local_date else None,
        "localHour": row.local_hour,
        "bookingDate": row.booking_date.isoformat() if row.booking_date else None,
        "hour": row.hour,
        "bookingId": row.booking_id,
        "bookingPoints": row.booking_points,
        "divisionId": row.division_id,
        "divisionName": row.division_name
        }
        for row in final_result
        ]

        return send_return_status(200, json.dumps(data))

    except Exception as e:
        print(e)
        return send_return_status(500, json.dumps({"error": str(e)}))


def post_slot(event, session):
    try:
        data = event.get("body")
        if not data:
            return send_return_status(400, json.dumps({"error": "Request body is missing"}))

        required_fields = ["entityId", "bookingDateId", "calenderRatePoints","divisionId"]   
        metaDetails = json.loads(data)
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result        

        entity_id = metaDetails.get("entityId")
        booking_date_id = metaDetails.get("bookingDateId")
        slot_reservation_points = metaDetails.get("calenderRatePoints")
        division_id = metaDetails.get("divisionId")
        booking_id = generate_booking_code()

        with session.begin():  # ensures commit/rollback
            result = session.execute(
                text("""
                    CALL book_slot(:entity_id, :booking_date_id, :points, :booking_id,:division_id)
                """),
                {
                    "entity_id": entity_id,
                    "booking_date_id": booking_date_id,
                    "points": slot_reservation_points,
                    "booking_id": booking_id,
                    "division_id":division_id
                }
            )
            results_dict = [dict(row._mapping) for row in result.fetchall()]
            row = results_dict[0]

            if row["status"] == "failed":
                return send_return_status(400, json.dumps(row))

            if row["status"] == "waiting":
                return send_return_status(200, json.dumps(row))

            if row["status"] == "confirmed":
                return send_return_status(201, json.dumps(row))
        return send_return_status(201, json.dumps(results_dict))

    except SQLAlchemyError as e:
        session.rollback()
        print("Database error:", e)
        return send_return_status(500, json.dumps({"error": str(e)}))

    except Exception as e:
        session.rollback()
        print("Unexpected error:", e)
        return send_return_status(500, json.dumps({"error": str(e)}))


def delete_slot(event,session):
    try:
        data = event.get("body")
        if not data:
            return send_return_status(400, json.dumps({"error": "Request body is missing"}))
         
        required_fields = ["bookingId"]   
        metaDetails=json.loads(data)
        validation_result = check_required_fields(metaDetails, required_fields)
        if validation_result is not True:
            return validation_result        
        booking_id = metaDetails.get("bookingId")      
        try:  
            with session.begin():  # ensures commit/rollback
                result = session.execute(
                text("""
                    CALL cancel_slot(:booking_id)
                """),
                {
                    "booking_id": booking_id
                }
            )
                results_dict = [dict(row._mapping) for row in result.fetchall()]
            return send_return_status(201, json.dumps(results_dict))
        except Exception as e:
            print("error",e)
            return send_return_status(500, json.dumps({"error": str(e)}))
    except Exception as e:
        print("error",e)
        return send_return_status(500, json.dumps({"error": str(e)})) 


def send_return_status(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        },
        "body": body,
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


def generate_booking_code():
    return f"BK{int(time.time() * 1000)}{random.randint(100,999)}"        
