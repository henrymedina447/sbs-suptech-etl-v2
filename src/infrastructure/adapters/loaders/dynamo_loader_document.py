import uuid

import boto3
from botocore.config import Config
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from application.ports.loader_metadata_port import LoaderMetadataPort
from domain.models.states.etl_base_state import EtlBaseState
from infrastructure.config.app_settings import AppSettings, get_app_settings


class DynamoLoaderMetadata(LoaderMetadataPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        self.table: Table = self._get_configuration()

    def _get_configuration(self) -> Table:
        _cfg = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=3,
            read_timeout=5
        )
        dynamo_resource: DynamoDBServiceResource = boto3.resource(
            "dynamodb",
            config=_cfg,
            region_name=self.app_settings.aws_settings.region
        )
        return dynamo_resource.Table(self.app_settings.table_settings.si_table)

    def save_metadata(self, document_type: str, data: list[EtlBaseState]) -> None:
        for d in data:
            metadata = d.model_dump(mode="json", exclude_none=True)
            metadata["document_type"] = document_type
            item = {
                "id": str(uuid.uuid4()),
                "metadata": metadata,
                "supervisedRecordId": d.record_id
            }
            self.table.put_item(Item=item)


