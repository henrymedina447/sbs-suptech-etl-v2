import logging
import asyncio
from typing import Any

import anyio
import boto3
from botocore.exceptions import ClientError
from mypy_boto3_textract import TextractClient
from mypy_boto3_textract.type_defs import (
    StartDocumentAnalysisResponseTypeDef,
    GetDocumentAnalysisResponseTypeDef,
    BlockTypeDef,
)

from application.ports.extractor_document_port import ExtractorDocumentPort
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_base_state import EtlBaseState
from infrastructure.adapters.extractors.textract.helpers.extract_async_helper import ExtractAsyncHelper
from infrastructure.config.app_settings import get_app_settings


class TextractExtractorDocument(ExtractorDocumentPort):
    def __init__(self):
        self.aws_settings = get_app_settings().aws_settings
        self.textract: TextractClient = boto3.client("textract", region_name=self.aws_settings.region)

    # ---------- ASYNC API del Port ----------
    async def extract_pipeline(self, document_data: DocumentContractState, origin: str) -> list[EtlBaseState]:
        job_id = await self._start_analysis(document_data.key)
        if job_id is None:
            return []

        results_from_analysis = await self._get_analysis_result(job_id)
        pages, blocks = type(self)._group_by_page(results_from_analysis)

        # Procesar contenido por página (paralelismo controlado en memoria)
        per_page = await ExtractAsyncHelper.extract_pages_async(
            pages=pages,
            blocks=blocks,
            batch_size=4,  # 1 si quieres estrictamente secuencial por página
            max_concurrency=4,  # techo de paralelismo
        )


        items_to_send: list[EtlBaseState] = []
        if origin == "inscripciones":
            for p in per_page:
                item = EtlBaseState(
                    record_id=document_data.record_id,
                    extract_success=True,
                    transform_success=False,
                    load_success=False,
                    document_content_total=p.get("text", ""),
                    document_content_llm=p.get("text", ""),
                )
                items_to_send.append(item)
        else:
            # Ejemplo simple: concatenar texto por página
            first_pages = "\n\n".join(p.get("text", "") for i, p in enumerate(per_page) if i < 20)
            full_text = "\n\n".join(p.get("text", "") for p in per_page)
            # print("full text", full_text)
            item = EtlBaseState(
                record_id=document_data.record_id,
                extract_success=True,
                transform_success=False,
                load_success=False,
                document_content_total=first_pages,
                document_content_llm=full_text,
            )
            items_to_send.append(item)

        return items_to_send

    # ------------------------------ Métodos privados ASYNC ------------------------------
    async def _start_analysis(self, file_key: str) -> str | None:
        """Envuelve start_document_analysis (sync) en un hilo y retorna JobId."""
        try:
            def _call() -> StartDocumentAnalysisResponseTypeDef:
                return self.textract.start_document_analysis(
                    DocumentLocation={"S3Object": {
                        "Bucket": get_app_settings().s3_settings.bucket,
                        "Name": file_key
                    }},
                    FeatureTypes=["TABLES", "LAYOUT"],
                )

            resp = await anyio.to_thread.run_sync(_call)
            return resp.get("JobId")
        except ClientError as e:
            logging.exception("error en start_analysis: %s", e)
            return None

    async def _get_document_analysis_page(
            self, job_id: str, next_token: str | None = None
    ) -> GetDocumentAnalysisResponseTypeDef:
        """Una página de resultados (maneja NextToken)."""

        def _call() -> GetDocumentAnalysisResponseTypeDef:
            kwargs = {"JobId": job_id}
            if next_token:
                kwargs["NextToken"] = next_token
            return self.textract.get_document_analysis(**kwargs)

        return await anyio.to_thread.run_sync(_call)

    async def _get_analysis_result(self, job_id: str) -> list[GetDocumentAnalysisResponseTypeDef]:
        """
        Polling asíncrono hasta que el Job termine, luego pagina todo el resultado.
        """
        resp = await self._get_document_analysis_page(job_id)
        logging.info("job status: %s", resp["JobStatus"])

        while resp["JobStatus"] == "IN_PROGRESS":
            await asyncio.sleep(5)  # NO usar time.sleep en async
            resp = await self._get_document_analysis_page(job_id)
            logging.info("job status: %s", resp["JobStatus"])

        all_responses: list[GetDocumentAnalysisResponseTypeDef] = [resp]
        while "NextToken" in all_responses[-1]:
            next_token = all_responses[-1]["NextToken"]  # type: ignore[index]
            next_page = await self._get_document_analysis_page(job_id, next_token)
            all_responses.append(next_page)

        return all_responses

    @staticmethod
    def _group_by_page(
            response: list[GetDocumentAnalysisResponseTypeDef],
    ) -> tuple[list[BlockTypeDef], list[BlockTypeDef]]:
        """
        Agrupa los blocks por página y retorna (pages, blocks).
        """
        blocks: list[BlockTypeDef] = [b for r in response for b in r.get("Blocks", [])]
        pages: list[BlockTypeDef] = [p for p in blocks if p.get("BlockType") == "PAGE"]
        return pages, blocks
