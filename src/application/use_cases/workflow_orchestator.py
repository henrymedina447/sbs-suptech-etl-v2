import logging
import uuid

from typing import Any, Literal
from langgraph.constants import START, END
from langgraph.graph import StateGraph

from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.ports.notification_port import NotificationPort

from application.use_cases.workflows.workflow_inscripciones import WorkflowInscripciones
from application.use_cases.workflows.workflow_polizas import WorkflowPolizas
from application.use_cases.workflows.workflow_tasaciones import WorkflowTasaciones

from domain.models.states.etl_orchestrator_state import (
    EtlOrchestatorState,
    EtlOrchestatorStateResult,
)
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.enums.document_type import DocumentType
from domain.models.notification import Notification, NotificationData

from infrastructure.config.app_settings import get_app_settings, AppSettings


class WorkflowOrchestator:
    def __init__(
        self,
        extractor: ExtractorDocumentPort,
        transformer: TransformDocumentPort,
        metadata_loader: LoaderMetadataPort,
        document_loader: LoaderDocumentPort,
        notification: NotificationPort,
    ):
        self.logger = logging.getLogger("app.workflows")
        self._extractor = extractor
        self._transformer = transformer
        self._metadata_loader = metadata_loader
        self._document_loader = document_loader
        self._notification = notification

        self.polizas_wf = WorkflowPolizas(
            self._extractor, self._transformer, self._metadata_loader, self._document_loader
        )
        self.inscripciones_wf = WorkflowInscripciones(
            self._extractor, self._transformer, self._metadata_loader, self._document_loader
        )
        self.tasaciones_wf = WorkflowTasaciones(
            self._extractor, self._transformer, self._metadata_loader, self._document_loader
        )
        self.app_settings: AppSettings = get_app_settings()
        self._graph = self._build()

    def _start_task(self, state: EtlOrchestatorState) -> dict[str, Any]:
        self.logger.info("Inicio de recolección de archivos")
        return {"documents": state.documents}

    def _select_flow(
        self, state: EtlOrchestatorState
    ) -> Literal["póliza", "inscripción", "tasación"]:
        flow = state.document_type
        print("flow", flow)
        if flow == DocumentType.REGISTRATION:
            return "inscripción"
        elif flow == DocumentType.APPRAISAL:
            return "tasación"
        else:  # TO DO: Handle default
            return "póliza"

    async def _polizas_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents
            if not total_documents:
                return {}

            results: list[EtlOrchestatorStateResult] = []
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                await self.polizas_wf.execute(doc)
                result = await self.polizas_wf.execute(doc)
                if result:
                    results.append(
                        EtlOrchestatorStateResult(
                            record_id=doc.record_id,
                            parent_id=doc.parent_id,
                            session_id=doc.session_id,
                        )
                    )

            return {"results": results}
        except Exception as e:
            self.logger.error(f"Error en polizas_flow: {str(e)}")
            return {}

    async def _inscripciones_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents
            if not total_documents:
                return {}

            results: list[EtlOrchestatorStateResult] = []
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                result = await self.inscripciones_wf.execute(doc)
                if result:
                    results.append(
                        EtlOrchestatorStateResult(
                            record_id=doc.record_id,
                            parent_id=doc.parent_id,
                            session_id=doc.session_id,
                        )
                    )

            return {"results": results}
        except Exception as e:
            self.logger.error(f"Error en inscripciones_flow: {str(e)}")
            return {}

    async def _tasaciones_flow(self, state: EtlOrchestatorState) -> dict[str, Any]:
        try:
            total_documents: list[DocumentContractState] = state.documents
            if not total_documents:
                return {}

            results: list[EtlOrchestatorStateResult] = []
            for index, doc in enumerate(total_documents):
                print(f"Ejecutando documento: {index + 1}")
                await self.tasaciones_wf.execute(doc)
                result = await self.tasaciones_wf.execute(doc)
                if result:
                    results.append(
                        EtlOrchestatorStateResult(
                            record_id=doc.record_id,
                            parent_id=doc.parent_id,
                            session_id=doc.session_id,
                        )
                    )

            return {"results": results}
        except Exception as e:
            self.logger.error(f"Error en inscripciones_flow: {str(e)}")
            return {}

    def _final_task(self, state: EtlOrchestatorState) -> dict[str, Any]:
        metadata_notification_type = "regulatory-compliance-prompts.insert-metadata"
        
        print("state", state.results)
        
        notifications = [
            Notification(
                id=str(uuid.uuid4()),
                message=NotificationData(
                    session_id=result.session_id,
                    type=metadata_notification_type,
                    data={"recordId": result.record_id, "parentId": result.parent_id},
                ),
            )
            for result in state.results
        ]
        self._notification.notify(notifications)
        return {}

    def _build(self):
        g = StateGraph(EtlOrchestatorState)
        g.add_node("start_task", self._start_task)
        g.add_node("polizas_flow", self._polizas_flow)
        g.add_node("inscripciones_flow", self._inscripciones_flow)
        g.add_node("tasaciones_flow", self._tasaciones_flow)
        g.add_node("final_task", self._final_task)

        g.add_edge(START, "start_task")
        g.add_conditional_edges(
            "start_task",
            self._select_flow,
            {
                "póliza": "polizas_flow",
                "inscripción": "inscripciones_flow",
                "tasación": "tasaciones_flow",
            },
        )
        g.add_edge("polizas_flow", "final_task")
        g.add_edge("inscripciones_flow", "final_task")
        g.add_edge("tasaciones_flow", "final_task")
        g.add_edge("final_task", END)
        return g.compile()

    async def execute(
        self, document_type: DocumentType, documents: list[DocumentContractState]
    ):  
        state = EtlOrchestatorState(
            document_type=document_type, documents=documents
        )
        output_raw = await self._graph.ainvoke(state)
        print(output_raw)
