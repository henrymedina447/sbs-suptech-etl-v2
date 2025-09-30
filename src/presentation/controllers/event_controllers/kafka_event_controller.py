import asyncio
import json
import logging
from typing import Any

from aiokafka import AIOKafkaConsumer
from pydantic import ValidationError

from domain.models.enums.document_type import DocumentType
from domain.models.states.document_contract_state import DocumentContractState
from infrastructure.bootstrap.container import build_workflow
from infrastructure.config.app_settings import KafkaSettings, get_app_settings
from presentation.dtos.requests.process_document import ProcessDocumentRequest

app_logger = logging.getLogger("app.environment")


class KafkaEventController:
    def __init__(self, max_concurrency: int = 8):
        self._wf = build_workflow()
        self._consumer: AIOKafkaConsumer | None = None
        self._stopping = asyncio.Event()
        self._sem = asyncio.Semaphore(max_concurrency)

    @staticmethod
    def _get_kafka_config() -> dict[str, Any]:
        k: KafkaSettings = get_app_settings().kafka_settings
        cfg: dict[str, Any] = {
            "bootstrap_servers": k.bootstrap_servers,
            "group_id": k.group_id,
            "security_protocol": k.security_protocol,
        }
        return cfg

    @staticmethod
    def _get_kafka_topic() -> str:
        k: KafkaSettings = get_app_settings().kafka_settings
        return k.topic

    @staticmethod
    async def create_consumer() -> AIOKafkaConsumer:
        cfg = KafkaEventController._get_kafka_config()
        topic = KafkaEventController._get_kafka_topic()
        consumer = AIOKafkaConsumer(topic, **cfg)
        await consumer.start()
        return consumer

    async def start(self) -> None:
        self._consumer = await KafkaEventController.create_consumer()
        asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping.set()
        if self._consumer:
            await self._consumer.stop()

    async def _loop(self) -> None:
        c = self._consumer
        try:
            while not self._stopping.is_set():
                batches = await c.getmany(timeout_ms=1000, max_records=10)
                tasks = []
                for _tp, msgs in batches.items():
                    document_requests: list[ProcessDocumentRequest] = []
                    for m in msgs:
                        text = m.value.decode("utf-8", errors="ignore")
                        data = json.loads(text) if text else {}
                        print(data)
                        process_document: ProcessDocumentRequest = (
                            ProcessDocumentRequest.model_validate(data, by_alias=True)
                        )
                        document_requests.append(process_document)
                    tasks.append(
                        asyncio.create_task(
                            self._handle(document_requests=document_requests)
                        )
                    )
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await c.stop()

    async def _handle(self, document_requests: list[ProcessDocumentRequest]) -> None:

        app_logger.info(f"Procesando mensajes", document_requests)

        async with self._sem:
            try:
                documents_by_type: dict[DocumentType, list[DocumentContractState]] = (
                    dict()
                )
                for item in document_requests:
                    document_contract_state = DocumentContractState(
                        record_id=item.record_id,
                        parent_id=item.parent_id,
                        key=item.key,
                        session_id=item.session_id,
                        document_type=item.document_type,
                        period_month=item.period_month,
                        period_year=item.period_year,
                    )
                    if item.document_type not in documents_by_type:
                        documents_by_type[item.document_type] = [
                            document_contract_state
                        ]
                    else:
                        documents_by_type[item.document_type].append(
                            document_contract_state
                        )

                print(documents_by_type)
                for [document_type, documents] in documents_by_type.items():
                    app_logger.info(
                        f"Ejecutando flow de: {document_type} - {len(documents)}"
                    )
                    result = await self._wf.execute(
                        document_type=document_type, documents=documents
                    )
                return {"status": "success"}

            except ValidationError as e:
                app_logger.exception(f"Error de validación: {str(e)}")
            except Exception as e:
                app_logger.exception(f"Error procesando mensaje kafka {str(e)}")
