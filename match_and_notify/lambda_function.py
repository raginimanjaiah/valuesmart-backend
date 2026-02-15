import os
from sqlalchemy import create_engine, select, and_, or_, insert, Index, Column, Integer, JSON, Enum, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

# --- Missing Table Model: The Match Tracker ---
class EnquirySupplierMatch(Base):
    __tablename__ = 'enquiry_supplier_matches'
    __table_args__ = (
        Index('idx_supplier_viewed', 'supplier_id', 'viewed'),
        {'schema': 'valuesmart'}
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    enquiry_id = Column(Integer, nullable=False)
    supplier_id = Column(Integer, nullable=False)
    notified = Column(Boolean, default=False)
    viewed = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())

class SupplierRegisteredEquipment(Base):
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



# DB Setup

#Database Config - Fetched from Environment Variables
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

def lambda_handler(event, context):
    enquiry_id = event.get('enquiry_id')
    session = SessionLocal()
    
    try:
        # 1. Find matches (O(1) search on buyer side)
        # We join with SupplierRegisteredEquipment on the specific IDs
        
        stmt = (
            select(SupplierRegisteredEquipment.supplier_id)
            .join(
                BuyerEnquiredEquipment,
                and_(
                    BuyerEnquiredEquipment.market_segment_id == SupplierRegisteredEquipment.market_segment_id,
                    BuyerEnquiredEquipment.unit_operation_id == SupplierRegisteredEquipment.unit_operation_id,
                    BuyerEnquiredEquipment.equipment_id == SupplierRegisteredEquipment.equipment_id,
                    BuyerEnquiredEquipment.capacity_id == SupplierRegisteredEquipment.capacity_id
                   
                )
            )
            .where(
                BuyerEnquiredEquipment.id == enquiry_id,
                BuyerEnquiredEquipment.archive == 'N',
                SupplierRegisteredEquipment.archive == 'N'
            )
        )
        
        supplier_ids = session.execute(stmt).scalars().all()

        # 2. Bulk Insert Matches
        if supplier_ids:
            match_entries = [
                {
                    "enquiry_id": enquiry_id,
                    "supplier_id": s_id,
                    "notified": True, # Assuming we send notification now
                    "viewed": False
                }
                for s_id in supplier_ids
            ]
            
            # Using core insert for speed
            session.execute(insert(EnquirySupplierMatch), match_entries)
            
            # 3. Trigger actual Notification (Email/SMS)
            # send_email_notifications(supplier_ids, enquiry_id)
            
            session.commit()
            print(f"Matched Enquiry {enquiry_id} with {len(supplier_ids)} suppliers")
            
    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()
