import logging
from typing import Any, Literal
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.poller_document_port import PollerDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.use_cases.workflows.workflow_inscripciones import WorkflowInscripciones
from application.use_cases.workflows.workflow_polizas import WorkflowPolizas
from application.use_cases.workflows.workflow_tasaciones import WorkflowTasaciones
from domain.models.enums.prefix_enum import PrefixEnum
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_orchestrator_state import EtlOrchestatorState
from infrastructure.config.app_settings import get_app_settings, AppSettings


class WorkflowOrchestator:
    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 loader: LoaderDocumentPort,
                 poller: PollerDocumentPort
                 ):
        self.logger = logging.getLogger("app.workflows")
        self._extractor = extractor
        self._transformer = transformer
        self._loader = loader
        self._poller = poller
        self.polizas_wf = WorkflowPolizas(self._extractor, self._transformer, self._loader)
        self.inscripciones_wf = WorkflowInscripciones(self._extractor, self._transformer, self._loader)
        self.tasaciones_wf = WorkflowTasaciones(self._extractor, self._transformer, self._loader)
        self.app_settings: AppSettings = get_app_settings()
        self._graph = self._build()

    def _start_task(self, state: EtlOrchestatorState) -> dict[str, Any]:
        self.logger.info("Inicio de recolección de archivos")
        bucket = self.app_settings.s3_settings.bucket
        documents: list[DocumentContractState] = self._poller.get_file_names(
            bucket_name=bucket,
            prefix_path=state.prefix.value,
            document_type="pdf"
        )
        return {
            "documents_with_contract": documents
        }

    def _select_flow(self, state: EtlOrchestatorState) -> Literal["póliza", "inscripción", "tasación"]:
        flow = state.prefix.value
        print("flow", flow)
        if flow == "Inscripciones/":
            return "inscripción"
        elif flow == "Tasaciones/":
            return "tasación"
        else:
            return "póliza"

    async def _polizas_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents_with_contract
            if not total_documents:
                return {}
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                await self.polizas_wf.execute(doc)
            return {}
        except Exception as e:
            self.logger.error(f"Error en polizas_flow: {str(e)}")
            return {}

    async def _inscripciones_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents_with_contract
            if not total_documents:
                return {}
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                await self.inscripciones_wf.execute(doc)
            return {}
        except Exception as e:
            self.logger.error(f"Error en inscripciones_flow: {str(e)}")
            return {}

    async def _tasaciones_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents_with_contract
            if not total_documents:
                return {}
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                await self.tasaciones_wf.execute(doc)
            return {}
        except Exception as e:
            self.logger.error(f"Error en inscripciones_flow: {str(e)}")
            return {}

    def _final_task(self, state: EtlOrchestatorState) -> dict[str, Any]:
        return {}

    def _build(self):
        g = StateGraph(EtlOrchestatorState)
        g.add_node("start_task", self._start_task)
        g.add_node("polizas_flow", self._polizas_flow)
        g.add_node("inscripciones_flow", self._inscripciones_flow)
        g.add_node("tasaciones_flow", self._tasaciones_flow)
        g.add_node("final_task", self._final_task)

        g.add_edge(START, "start_task")
        g.add_conditional_edges("start_task", self._select_flow, {
            "póliza": "polizas_flow",
            "inscripción": "inscripciones_flow",
            "tasación": "tasaciones_flow"
        })
        g.add_edge("polizas_flow", "final_task")
        g.add_edge("inscripciones_flow", "final_task")
        g.add_edge("tasaciones_flow", "final_task")
        g.add_edge("final_task", END)
        return g.compile()

    async def execute(self, prefix: PrefixEnum):
        state = EtlOrchestatorState(prefix=prefix)
        output_raw = await self._graph.ainvoke(state)
        print(output_raw)
