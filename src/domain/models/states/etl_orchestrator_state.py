import operator
from typing import Literal, Annotated

from pydantic import BaseModel, Field

from domain.models.enums.prefix_enum import PrefixEnum
from domain.models.states.document_contract_state import DocumentContractState


class EtlOrchestatorState(BaseModel):
    prefix: PrefixEnum = Field(description="indica que elementos se recorrerán")
    documents_with_contract: Annotated[list[DocumentContractState], operator.add] = Field(
        description="Todos los documentos ya procesados",
        default_factory=list
    )
