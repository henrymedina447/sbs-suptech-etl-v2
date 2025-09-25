from application.ports.loader_document_port import LoaderDocumentPort
from infrastructure.config.app_settings import AppSettings, get_app_settings
from mypy_boto3_s3.service_resource import Bucket

import boto3

class S3LoaderDocument(LoaderDocumentPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        self.bucket: Bucket = self._get_cofiguration()
        
    def _get_cofiguration(self) -> Bucket:
        s3 = boto3.resource("s3", region_name=self.app_settings.aws_settings.region)
        return s3.Bucket(self.app_settings.s3_settings.bucket)
        
    
    def save_document(self, key: str, data: bytes) -> None:
        self.bucket.put_object(Key=key, Body=data)