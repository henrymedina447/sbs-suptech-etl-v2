import uuid

import boto3

from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from mypy_boto3_dynamodb.service_resource import DynamoDBServiceResource, Table
from application.ports.loader_metadata_port import LoaderMetadataPort
from domain.models.states.etl_base_state import EtlBaseState
from infrastructure.config.app_settings import AppSettings, get_app_settings
from typing import Any


class DynamoLoaderMetadata(LoaderMetadataPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        dynamo_resource = self._get_configuration()
        self.si_table: Table = dynamo_resource.Table(
            self.app_settings.table_settings.si_table
        )

    def _get_configuration(self) -> Table:
        _cfg = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=3,
            read_timeout=5,
        )
        dynamo_resource: DynamoDBServiceResource = boto3.resource(
            "dynamodb", config=_cfg, region_name=self.app_settings.aws_settings.region
        )
        return dynamo_resource

    def save_metadata(self, document_type: str, data: list[EtlBaseState]) -> None:

        for d in data:
            print(f"Saving metadata for {document_type}", d)

            query_output = self.si_table.query(
                KeyConditionExpression=Key("supervisoryRecordId").eq(d.record_id),
                IndexName="supervisoryRecordId-index",
                Limit=1,
            )
            existing_metadata = query_output["Items"][0]

            raw_data = d.model_dump(mode="json", exclude_none=True)
            metadata = {
                "period_year": raw_data["period_year"],
                "period_month": raw_data["period_month"],
                "file_name": raw_data["file_name"],
            }
            
            metadata.update(raw_data["metadata"])
            metadata.update(existing_metadata["metadata"])

            self.si_table.update_item(
                Key={
                    "id": metadata["id"],
                },
                UpdateExpression="set metadata = :metadata",
                ExpressionAttributeValues={
                    ":metadata": metadata,
                },
                ReturnValues="UPDATED_NEW",
            )
