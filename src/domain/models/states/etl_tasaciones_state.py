from pydantic import Field

from domain.models.states.etl_base_state import EtlBaseState


class EtlTasacionesState(EtlBaseState):
    expert_warranty_name: str = Field(description="Indica el nombre del perito")
    tasacion_date: str = Field(description="Indica la fecha de la tasación")
    commercial_value: str = Field(description="Indica el valor comercial en soles (PEN)")
    realization_value: str = Field(description="Indica el valor de realización en soles (PEN)")
    tasacion_owner: str = Field(description="Indica el nombre del propietario de la tasación")
