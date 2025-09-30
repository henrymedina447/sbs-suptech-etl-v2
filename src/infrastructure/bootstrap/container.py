from application.use_cases.workflow_orchestator import WorkflowOrchestator
from infrastructure.adapters.extractors.textract.textract_extractor_document import TextractExtractorDocument
from infrastructure.adapters.loaders.dynamo_loader_document import DynamoLoaderDocument
from infrastructure.adapters.notification.sqs_notification import SqsNotification
from infrastructure.adapters.transformers.bed_rock_transformer_document import BedRockTransformerDocument


def build_workflow() -> WorkflowOrchestator:
    extractor = TextractExtractorDocument()
    transformer = BedRockTransformerDocument()
    loader = DynamoLoaderDocument()
    notification = SqsNotification()
    return WorkflowOrchestator(extractor, transformer, loader, notification)
