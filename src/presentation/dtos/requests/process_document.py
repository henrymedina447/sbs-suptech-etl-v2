from pydantic import BaseModel, Field

from domain.models.enums.document_type import DocumentType

class ProcessDocumentRequest(BaseModel):
    record_id: str = Field(..., alias="recordId")
    parent_id: str = Field(..., alias="parentId")
    key: str = Field(..., alias="key")
    session_id: str = Field(..., alias="sessionId")
    document_type: DocumentType = Field(..., alias="documentType")
    period_month: str = Field(..., alias="periodMonth")
    period_year: str = Field(..., alias="periodYear")
 
