import logging
from typing import Any

import anyio

from application.ports.extractor_document_port import ExtractorDocumentPort
from application.ports.loader_metadata_port import LoaderMetadataPort
from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.ports.notification_port import NotificationPort

from application.use_cases.workflows.workflow_base import WorkflowBase


from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_base_state import EtlBaseState
from domain.models.states.etl_tasaciones_state import EtlTasacionesState

from domain.services.workflow_service import WorkflowService


class WorkflowTasaciones(WorkflowBase):
    def __init__(
        self,
        extractor: ExtractorDocumentPort,
        transformer: TransformDocumentPort,
        metadata_loader: LoaderMetadataPort,
        document_loader: LoaderDocumentPort,
    ):
        super().__init__(extractor, transformer, metadata_loader, document_loader)
        self.logger = logging.getLogger("app.workflows")
        self.document_data: DocumentContractState | None = None

    async def _extract(self, state: EtlTasacionesState) -> dict[str, Any]:
        try:
            self.logger.info("Iniciando el proceso de extracción de tasaciones")
            items: list[EtlBaseState] = await self._extractor.extract_pipeline(
                document_data=self.document_data, origin="tasaciones"
            )
            if len(items) == 0:
                return {"extract_success": False}
            item = items[0]
            return {
                "extract_success": True,
                "record_id": self.document_data.record_id,
                "period_month": self.document_data.period_month,
                "period_year": self.document_data.period_year,
                "document_content_total": item.document_content_total,
                "document_content_llm": item.document_content_llm,
            }
        except Exception as e:
            self.logger.error(f"Error en la extracción de tasaciones: {str(e)}")
            return {"extract_success": False}

    async def _transform(self, state: EtlTasacionesState) -> dict[str, Any]:
        try:
            extract_success = state.extract_success
            if not extract_success:
                return {}
            document_llm = state.document_content_llm
            item: EtlTasacionesState | None = await anyio.to_thread.run_sync(
                self._transformer.llm_caller_tasaciones, document_llm
            )
            print("item", item)
            if item is None:
                return {"transform_success": False}
            return {
                "transform_success": True,
                "expert_warranty_name": item.expert_warranty_name,
                "tasacion_date": WorkflowService.refine_dates(item.tasacion_date),
                "commercial_value": item.commercial_value,
                "realization_value": item.realization_value,
                "tasacion_owner": item.tasacion_owner,
            }
        except Exception as e:
            self.logger.error(f"Error en transformación de tasaciones: {str(e)}")
            return {"transform_success": False}

    async def _load(self, state: EtlTasacionesState) -> dict[str, Any]:
        try:
            self.logger.info("Iniciando el proceso de carga de tasaciones")
            transform_success = state.transform_success
            if not transform_success:
                return {}
            
            text_key = f"txt/{state.record_id}.txt"
            await anyio.to_thread.run_sync(
                self._document_loader.save_document,
                text_key,
                state.document_content_total.encode("utf-8"),
            )
            state.document_content_total = None
            state.document_content_llm = None
            
            print("state en load", state)

            await anyio.to_thread.run_sync(
                self._metadata_loader.save_metadata, "tasaciones", [state]
            )
            
            return {"load_success": True}
        except Exception as e:
            self.logger.info(f"Error en carga de tasaciones: {str(e)}")
            return {"load_success": False}

    async def _final_task(self, state: EtlTasacionesState) -> dict[str, Any]:
        pass

    async def execute(self, data: DocumentContractState) -> EtlTasacionesState:
        self.document_data = data
        state: EtlTasacionesState = EtlTasacionesState(record_id=data.record_id)
        output_raw = await self._graph.ainvoke(state)
        output = EtlTasacionesState.model_validate(output_raw)
        return output.transform_success == True and output.load_success == True and output.extract_success == True
