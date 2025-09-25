from pydantic import Field

from domain.models.states.etl_base_state import EtlBaseState


class EtlTasacionesState(EtlBaseState):
    expert_warranty_name: str | None = Field(description="Indica el nombre del perito", default=None)
    tasacion_date: str | None = Field(description="Indica la fecha de la tasación", default=None)
    commercial_value: str | None = Field(description="Indica el valor comercial en soles (PEN)", default=None)
    realization_value: str | None = Field(description="Indica el valor de realización en soles (PEN)", default=None)
    tasacion_owner: str | None = Field(description="Indica el nombre del propietario de la tasación", default=None)
