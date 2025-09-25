from application.use_cases.workflow_orchestator import WorkflowOrchestator
from infrastructure.adapters.extractors.textract.textract_extractor_document import TextractExtractorDocument
from infrastructure.adapters.loaders.dynamo_loader_document import DynamoLoaderMetadata
from infrastructure.adapters.loaders.s3_loader_document import S3LoaderDocument
from infrastructure.adapters.notification.sqs_notification import SqsNotification
from infrastructure.adapters.transformers.bed_rock_transformer_document import BedRockTransformerDocument


def build_workflow() -> WorkflowOrchestator:
    extractor = TextractExtractorDocument()
    transformer = BedRockTransformerDocument()
    metadata_loader = DynamoLoaderMetadata()
    document_loader = S3LoaderDocument()

    notification = SqsNotification()
    return WorkflowOrchestator(extractor, transformer, metadata_loader, document_loader, notification)
