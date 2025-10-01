import os.path
import boto3
from mypy_boto3_s3 import S3Client
from application.ports.poller_document_port import PollerDocumentPort
from domain.models.states.document_contract_state import DocumentContractState
from infrastructure.config.app_settings import get_app_settings


class S3PollerDocument(PollerDocumentPort):
    def __init__(self):
        self.app_settings = get_app_settings()
        self.s3_client: S3Client = boto3.client("s3", self.app_settings.aws_settings.region)

    def get_file_names(self, bucket_name: str, prefix_path: str, document_type: str = "pdf",
                       position: int | None = None) -> list[DocumentContractState]:
        """
        Obtiene todos los nombres de los archivos y retorna el tipo seleccionado
        """
        results: list[DocumentContractState] = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/"):
                    continue
                if not key.lower().endswith(f".{document_type.lower()}"):
                    continue
                # Se retira el prefijo base ("ej: Pólizas/")
                rel_path = key.removeprefix(prefix_path)
                # folder = parte antes del archivo ("ej: Mayo 2023")
                folder = os.path.dirname(rel_path)
                # document_name = parte después del folder ("ej: documento.pdf")
                file_name = os.path.basename(rel_path)
                # Se intenta parsear mes y año
                try:

                    period_month, period_year = folder.split(" ")
                except Exception as e:
                    print(f"Error en get_file_names {str(e)}")
                    period_month, period_year = None, None
                new_element = DocumentContractState(
                    key=key,
                    folder=folder,
                    document_name=file_name,
                    period_month=period_month,
                    period_year=period_year,
                )

                results.append(
                    new_element
                )
        if position is not None:
            return [results[position]]
        return results

