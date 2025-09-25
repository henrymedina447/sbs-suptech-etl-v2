from application.ports.transform_document_port import TransformDocumentPort
from domain.models.states.etl_inscripciones_state import EtlInscripcionesState
from domain.models.states.etl_polizas_state import EtlPolizasState
from domain.models.states.etl_tasaciones_state import EtlTasacionesState


class BedRockTransformerDocument(TransformDocumentPort):
    def llm_caller_polizas(self, document_content: str) -> EtlPolizasState | None:
        pass

    def llm_caller_inscripciones(self, **kwargs) -> EtlInscripcionesState | None:
        pass

    def llm_caller_tasaciones(self, **kwargs) -> EtlTasacionesState | None:
        pass
