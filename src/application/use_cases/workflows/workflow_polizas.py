import logging
from typing import Any

from application.ports.loader_document_port import LoaderDocumentPort
from application.ports.transform_document_port import TransformDocumentPort
from application.ports.extractor_document_port import ExtractorDocumentPort
from application.use_cases.workflows.workflow_base import WorkflowBase
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_base_state import EtlBaseState
from domain.models.states.etl_polizas_state import EtlPolizasState
import anyio

from domain.services.workflow_service import WorkflowService


class WorkflowPolizas(WorkflowBase):

    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 loader: LoaderDocumentPort
                 ):
        super().__init__(extractor, transformer, loader)
        self.logger = logging.getLogger("app.workflows")
        self.document_data: DocumentContractState | None = None

    async def _extract(self, state: EtlPolizasState) -> dict[str, Any]:
        try:
            items: list[EtlBaseState] = await self._extractor.extract_pipeline(document_data=self.document_data,
                                                                               origin="polizas")

            if len(items) == 0:
                return {
                    "extract_success": False
                }
            item = items[0]
            return {
                "extract_success": True,
                "document_name": self.document_data.document_name,
                "period_month": WorkflowService.refine_month(self.document_data.period_month),
                "period_year": WorkflowService.refine_year(self.document_data.period_year),
                "document_content_total": item.document_content_total,
                "document_content_llm": item.document_content_llm
            }
        except Exception as e:
            self.logger.error(f"Error en extracción de pólizas: {str(e)}")
            return {
                "extract_success": False
            }

    async def _transform(self, state: EtlPolizasState) -> dict[str, Any]:
        try:
            extract_success = state.extract_success
            if not extract_success:
                return {}
            document_llm = state.document_content_llm
            item: EtlPolizasState | None = await anyio.to_thread.run_sync(
                self._transformer.llm_caller_polizas,
                document_llm
            )
            if item is None:
                return {
                    "transform_success": False
                }
            return {
                "transform_success": True,
                "policy_number": item.policy_number,
                "policy_name": item.policy_name,
                "policy_start_date": WorkflowService.refine_dates(item.policy_start_date),
                "policy_end_date": WorkflowService.refine_dates(item.policy_end_date)
            }
        except Exception as e:
            self.logger.error(f"Error en transformación de pólizas: {str(e)}")
            return {
                "transform_success": False
            }

    async def _load(self, state: EtlPolizasState) -> dict[str, Any]:
        try:
            self.logger.info("Iniciando el proceso de carga de pólizas")
            transform_success = state.transform_success
            if not transform_success:
                return {}
            await anyio.to_thread.run_sync(self._loader.save_metadata, "polizas", [state])
            return {
                "load_success": True
            }
        except Exception as e:
            self.logger.info(f"Error en carga: {str(e)}")
            return {
                "load_success": False
            }

    async def _final_task(self, state: EtlPolizasState) -> dict[str, Any]:
        return {}

    async def execute(self, data: DocumentContractState) -> EtlPolizasState:
        self.document_data = data
        state: EtlPolizasState = EtlPolizasState(document_name=data.document_name)
        output_raw = await self._graph.ainvoke(state)
        output = EtlPolizasState.model_validate(output_raw)
        return output
