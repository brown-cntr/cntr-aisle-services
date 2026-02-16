from datetime import datetime, date
from typing import Optional
from enum import Enum
from uuid import UUID
from pydantic import BaseModel, Field

class BillBody(str, Enum):
    """Bill body type enum"""
    HOUSE = "house"
    SENATE = "senate"
    ASSEMBLY = "assembly"

class Bill(BaseModel):
    """Legislative bill data model"""
    id: Optional[UUID] = None
    external_id: str = Field(..., description="External bill ID (e.g., LegiScan bill ID)")
    title: str
    state: str
    year: int
    bill_number: str
    body: BillBody = Field(..., description="Bill body type: house, senate, or assembly")
    summary: Optional[str] = None
    url: Optional[str] = None
    legiscan_url: Optional[str] = None
    legiscan_id: Optional[int] = None
    openstates_id: Optional[str] = None
    openstates_url: Optional[str] = None
    version_date: Optional[date] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    full_text: Optional[str] = None
    
    class Config:
        from_attributes = True
        use_enum_values = True