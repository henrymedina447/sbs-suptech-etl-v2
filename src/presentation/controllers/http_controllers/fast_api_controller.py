import logging

from fastapi import FastAPI, Depends

from application.use_cases.workflow_orchestator import WorkflowOrchestator

from domain.models.enums.document_type import DocumentType
from domain.models.states.document_contract_state import DocumentContractState

from infrastructure.adapters.extractors.textract.textract_extractor_document import (
    TextractExtractorDocument,
)
from infrastructure.adapters.loaders.dynamo_loader_document import DynamoLoaderDocument
from infrastructure.adapters.transformers.bed_rock_transformer_document import (
    BedRockTransformerDocument,
)
from infrastructure.adapters.notification.sqs_notification import SqsNotification


from presentation.dtos.requests.process_document import (
    ProcessDocumentRequest,
    ProcessDocumentRequestItem,
)

app = FastAPI(title="SBS ETL API")

app_logger = logging.getLogger("app.environment")


def get_factory() -> WorkflowOrchestator:
    extractor = TextractExtractorDocument()
    transformer = BedRockTransformerDocument()
    loader = DynamoLoaderDocument()
    notification = SqsNotification()
    return WorkflowOrchestator(extractor, transformer, loader, notification)


async def execute_workflows(
    wf: WorkflowOrchestator,
    documents_by_type: dict[DocumentType, list[DocumentContractState]],
):
    for [document_type, documents] in documents_by_type.items():
        app_logger.info(f"Ejecutando flow de: {document_type} - {len(documents)}")
        await wf.execute(document_type=document_type, documents=documents)


@app.post("/start-etl")
async def run_etl(
    process_document: ProcessDocumentRequest,
    wf: WorkflowOrchestator = Depends(get_factory),
):
    documents_by_type: dict[DocumentType, list[DocumentContractState]] = dict()
    for item in process_document.documents:
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
            documents_by_type[item.document_type] = [document_contract_state]
        else:
            documents_by_type[item.document_type].append(document_contract_state)

    for [document_type, documents] in documents_by_type.items():
        app_logger.info(f"Ejecutando flow de: {document_type} - {len(documents)}")
        result = await wf.execute(document_type=document_type, documents=documents)
    return {"status": "success"}
