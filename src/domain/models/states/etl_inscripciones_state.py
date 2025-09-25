import operator
from typing import Annotated

from pydantic import Field, BaseModel

from domain.models.states.etl_base_state import EtlBaseState


class EtlInscripcionChild(EtlBaseState):
    inscription_number: str | None = Field(
        description="Hace referencia al número de la inscripción o también conocido como "
                    "número de partida", default=None)
    legal_name: str | None = Field(description="Hace referencia a la razón social / beneficiario de la inscripción",
                                   default=None)
    inscription_date: str | None = Field(description="Hace referencia a la fecha en la que se realizó la inscripción",
                                         default=None)


class EtlInscripcionesState(EtlBaseState):
    children_extracted: Annotated[list[EtlInscripcionChild], operator.add] = Field(
        description="Son todas las inscripciones obtenidas del documento y su contenido extraído",
        default_factory=list)
    children_transformed: Annotated[list[EtlInscripcionChild], operator.add] = Field(
        description="Son todas las inscripciones obtenidas del documento y su contenido transformado",
        default_factory=list)

