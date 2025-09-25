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


class WorkflowPolizas(WorkflowBase):

    def __init__(self,
                 extractor: ExtractorDocumentPort,
                 transformer: TransformDocumentPort,
                 loader: LoaderDocumentPort
                 ):
        super().__init__(extractor, transformer, loader)
        self.logger = logging.getLogger("app.workflows")
        self.document_data: DocumentContractState | None = None

    async def _extract(self, state: EtlBaseState) -> dict[str, Any]:
        try:

            item: EtlBaseState | None = await anyio.to_thread.run_sync(
                self._extractor.extract_pipeline,
                self.document_data,
                "polizas"
            )
            if item is None:
                return {
                    "extract_success": False
                }
            return {
                "extract_success": True,
                "document_content_total": item.document_content_total,
                "document_content_llm": item.document_content_llm
            }
        except Exception as e:
            self.logger.error(f"Error en extracción: {str(e)}")
            return {
                "extract_success": False
            }

    async def _transform(self, state: EtlBaseState) -> dict[str, Any]:
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
                "policy_start_date": item.policy_start_date,
                "policy_end_date": item.policy_end
            }
        except Exception as e:
            self.logger.info(f"Error en transformación: {str(e)}")
            return {
                "transform_success": False
            }

    async def _load(self, state: EtlBaseState) -> dict[str, Any]:
        try:
            transform_success = state.transform_success
            if not transform_success:
                return {}
            await anyio.to_thread.run_sync(self._loader.save_metadata, "polizas", state)
            return {
                "load_success": True
            }
        except Exception as e:
            self.logger.info(f"Error en carga: {str(e)}")
            return {
                "load_success": False
            }

    async def _final_task(self, state: EtlBaseState) -> dict[str, Any]:
        return {}

    async def execute(self, data: DocumentContractState) -> EtlPolizasState:
        self.document_data = data
        state: EtlPolizasState = EtlPolizasState(document_name=data.document_name)
        output_raw = await self._graph.ainvoke(state)
        output = EtlPolizasState.model_validate(output_raw)
        return output
