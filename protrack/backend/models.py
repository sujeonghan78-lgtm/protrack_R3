from pydantic import BaseModel
from typing import Optional, Any


class ProcessUpdate(BaseModel):
    시방출도일: Optional[Any] = None
    자재확인일: Optional[Any] = None
    자재예상일: Optional[Any] = None
    자재입고일: Optional[Any] = None
    생산완료일: Optional[Any] = None
    검사예상일: Optional[Any] = None
    포장완료예정일: Optional[Any] = None
    포장완료일: Optional[Any] = None
    최종납기일: Optional[Any] = None
    비고: Optional[str] = None

    class Config:
        # Allow any field name mapping
        populate_by_name = True


class PaginationParams(BaseModel):
    page: int = 1
    page_size: int = 50
