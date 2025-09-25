from pydantic import Field

from domain.models.states.etl_base_state import EtlBaseState


class EtlPolizasState(EtlBaseState):
    policy_number: str | None = Field(description="Número de la póliza", default=None)
    policy_name: str | None = Field(description="A nombre de quien está la póliza, la razón social", default=None)
    policy_start_date: str | None = Field(description="Fecha de inicio de la póliza", default=None)
    policy_end_date: str | None = Field(description="Fecha fin de la póliza", default=None)
