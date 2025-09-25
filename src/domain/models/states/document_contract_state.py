from pydantic import BaseModel, Field
from domain.models.enums.document_type import DocumentType


class DocumentContractState(BaseModel):
    record_id: str = Field(...)
    parent_id: str = Field(...)
    key: str = Field(...)
    session_id: str = Field(...)
    document_type: DocumentType = Field(...)
    period_month: str = Field(...)
    period_year: str = Field(...)
