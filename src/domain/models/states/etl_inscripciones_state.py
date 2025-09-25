from pydantic import Field

from domain.models.states.etl_base_state import EtlBaseState


class EtlInscripcionesState(EtlBaseState):
    inscription_number: str = Field(description="Hace referencia al número de la inscripción o también conocido como "
                                                "número de partida")
    legal_name: str = Field(description="Hace referencia a la razón social / beneficiario de la inscripción")
    inscription_date: str = Field(description="Hace referencia a la fecha en la que se realizó la inscripción")
