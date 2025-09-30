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
from domain.models.states.etl_inscripciones_state import (
    EtlInscripcionesState,
    EtlInscripcionChild,
)
from domain.models.states.etl_polizas_state import EtlPolizasState
from domain.services.workflow_service import WorkflowService


class WorkflowInscripciones(WorkflowBase):
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

    async def _extract(self, state: EtlInscripcionesState) -> dict[str, Any]:
        try:
            items: list[EtlBaseState] = await self._extractor.extract_pipeline(
                document_data=self.document_data, origin="inscripciones"
            )
            children: list[EtlInscripcionChild] = (
                WorkflowService.resolve_inscripciones_children(items, state)
            )
            if len(items) == 0:
                return {"extract_success": False}

            return {"extract_success": True, "children_extracted": children}
        except Exception as e:
            self.logger.error(f"Error en la extracción de inscripciones: {str(e)}")
            return {"extract_success": False}

    async def _transform(self, state: EtlInscripcionesState) -> dict[str, Any]:
        try:
            extract_success = state.extract_success
            if not extract_success:
                return {}
            children_extracted: list[EtlInscripcionChild] = state.children_extracted
            print("children", len(children_extracted))
            children_transformed: list[EtlInscripcionChild] = []
            for index, child in enumerate(children_extracted):
                print("index child", index)
                item = await self._transform_unit(child)
                print("item", item)
                children_transformed.append(item)

            return {
                "transform_success": True,
                "children_transformed": children_transformed,
            }

        except Exception as e:
            self.logger.error(f"Error en la transformación de inscripciones: {str(e)}")
            return {"transform_success": False}

    async def _load(self, state: EtlInscripcionesState) -> dict[str, Any]:
        try:
            self.logger.info("Iniciando el proceso de carga de inscripciones")
            transform_success = state.transform_success
            if not transform_success:
                return {}

            for child in state.children_transformed:
                text_key = f"txt/{state.record_id}.txt"
                await anyio.to_thread.run_sync(
                    self._document_loader.save_document,
                    text_key,
                    child.document_content_total.encode("utf-8"),
                )
                child.document_content_total = None
                child.document_content_llm = None

            await anyio.to_thread.run_sync(
                self._metadata_loader.save_metadata,
                "inscripciones",
                state.children_transformed,
            )

            return {"load_success": True}
        except Exception as e:
            self.logger.error(f"Error en la carga de inscripciones: {str(e)}")
            return {"load_success": False}

    def _final_task(self, state: EtlInscripcionesState) -> dict[str, Any]:
        return {}

    async def execute(self, data: DocumentContractState) -> bool:
        self.document_data = data
        state: EtlInscripcionesState = EtlInscripcionesState(
            record_id=data.record_id,
            period_year=data.period_year,
            period_month=data.period_month,
        )
        output_raw = await self._graph.ainvoke(state)
        output = EtlInscripcionesState.model_validate(output_raw)
        return (
            output.transform_success == True
            and output.load_success == True
            and output.extract_success == True
        )

    # -------------------------- Métodos complementarios al flujo
    async def _transform_unit(
        self, child: EtlInscripcionChild
    ) -> EtlInscripcionChild | None:
        try:
            document_llm = child.document_content_llm
            item: EtlInscripcionChild | None = await anyio.to_thread.run_sync(
                self._transformer.llm_caller_inscripciones, document_llm
            )
            if item is None:
                child.transform_success = False
                return None
            return EtlInscripcionChild(
                **child.model_dump(
                    exclude={
                        "inscription_number",
                        "legal_name",
                        "inscription_date",
                        "transform_success",
                    }
                ),
                inscription_number=item.inscription_number,
                legal_name=item.legal_name,
                inscription_date=item.inscription_date,
                transform_success=True,
            )

        except Exception as e:
            self.logger.error(
                f"Error en el proceso de de carga de una unida en bedrock: {str(e)}"
            )
            return None
