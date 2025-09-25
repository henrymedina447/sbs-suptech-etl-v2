import logging

from fastapi import FastAPI, Depends

from application.use_cases.workflow_orchestator import WorkflowOrchestator
from domain.models.enums.prefix_enum import PrefixEnum
from domain.models.states.etl_orchestrator_state import EtlOrchestatorState
from infrastructure.adapters.extractors.textract_runner_document import TextractExtractorDocument
from infrastructure.adapters.loaders.dynamo_loader_document import DynamoLoaderDocument
from infrastructure.adapters.pollers.s3_poller_document import S3PollerDocument
from infrastructure.adapters.transformers.bed_rock_transformer_document import BedRockTransformerDocument

app = FastAPI(title="SBS ETL API")

app_logger = logging.getLogger("app.environment")


def get_factory() -> WorkflowOrchestator:
    extractor = TextractExtractorDocument()
    transformer = BedRockTransformerDocument()
    poller = S3PollerDocument()
    loader = DynamoLoaderDocument()
    return WorkflowOrchestator(extractor, transformer, loader, poller)


@app.get("/start-etl")
async def run_etl(wf: WorkflowOrchestator = Depends(get_factory)):
    prefixes: list[PrefixEnum] = [PrefixEnum.polizas, PrefixEnum.inscripciones, PrefixEnum.tasaciones]
    for prefix in prefixes:
        app_logger.info(f"Ejecutando flow de: {prefix}")
        result = await wf.execute(prefix=prefix)
    return {"status": "success"}
