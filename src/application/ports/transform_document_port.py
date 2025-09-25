from abc import abstractmethod, ABC
from domain.models.states.etl_inscripciones_state import EtlInscripcionesState, EtlInscripcionChild
from domain.models.states.etl_polizas_state import EtlPolizasState
from domain.models.states.etl_tasaciones_state import EtlTasacionesState


class TransformDocumentPort(ABC):

    @abstractmethod
    def llm_caller_polizas(self, **kwargs) -> EtlPolizasState | None:
        ...

    @abstractmethod
    def llm_caller_inscripciones(self, **kwargs) -> EtlInscripcionChild | None:
        ...

    @abstractmethod
    def llm_caller_tasaciones(self, **kwargs) -> EtlTasacionesState | None:
        ...
