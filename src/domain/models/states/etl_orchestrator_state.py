import operator
from typing import Annotated

from pydantic import BaseModel, Field

from domain.models.enums.document_type import DocumentType
from domain.models.states.document_contract_state import DocumentContractState


class EtlOrchestatorStateResult(BaseModel):
    record_id: str = Field(description="ID del documento")
    parent_id: str = Field(description="ID del documento padre")
    session_id: str = Field(description="ID de la sesión")


class EtlOrchestatorState(BaseModel):
    document_type: DocumentType = Field(
        description="tipo de documento que se está procesando"
    )
    documents: list[DocumentContractState] = Field(
        description="indica que elementos se recorrerán"
    )
    results: list[EtlOrchestatorStateResult] | None = Field(
        description="indica que elementos se recorrieron", default=None
    )
