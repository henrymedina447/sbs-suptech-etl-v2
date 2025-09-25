import logging
import time

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_textract import TextractClient
from mypy_boto3_textract.type_defs import StartDocumentAnalysisResponseTypeDef, GetDocumentAnalysisResponseTypeDef

from application.ports.extractor_document_port import ExtractorDocumentPort
from domain.models.states.document_contract_state import DocumentContractState
from domain.models.states.etl_base_state import EtlBaseState
from infrastructure.config.app_settings import get_app_settings


class TextractExtractorDocument(ExtractorDocumentPort):
    def __init__(self):
        self.aws_settings = get_app_settings().aws_settings
        self.textract: TextractClient = boto3.client("textract", region_name=self.aws_settings.region)

    def extract_pipeline(self, document_data: DocumentContractState, origin: str) -> EtlBaseState | None:
        job_id = self._start_analysis(document_data.key)
        if job_id is None:
            return None
        results_from_analysis = self._get_analysis_result(job_id)
        return EtlBaseState(
            document_name=document_data.document_name,
            extract_success=False,
            transform_success=False,
            load_success=False,
            document_content_total=None,
            document_content_llm=None
        )

    # ------------------------------ Métodos preparativos ------------------------------
    def _start_analysis(self, document_name: str) -> str | None:
        """
        Retorna un job Id para dar seguimiento al análisis del documento
        """
        try:
            response: StartDocumentAnalysisResponseTypeDef = self.textract.start_document_analysis(
                DocumentLocation={
                    "S3Object": {
                        "Bucket": get_app_settings().s3_settings.bucket,
                        "Name": document_name
                    },
                },
                FeatureTypes=["TABLES", "LAYOUT"]
            )
            return response.get("JobId", None)
        except ClientError as e:
            logging.error(f"error en start_analysis: {str(e)}")
            return None

    def _get_analysis_result(self, job_id: str) -> list[GetDocumentAnalysisResponseTypeDef]:
        time.sleep(1)
        response: GetDocumentAnalysisResponseTypeDef = self.textract.get_document_analysis(JobId=job_id)
        logging.info(f"job status: {response["JobStatus"]}")
        while response["JobStatus"] == "IN_PROGRESS":
            time.sleep(5)
            response: GetDocumentAnalysisResponseTypeDef = self.textract.get_document_analysis(
                JobId=job_id
            )
            print(f"job status: {response['JobStatus']}")
        all_responses: list[GetDocumentAnalysisResponseTypeDef] = [response]
        while True:
            if "NextToken" not in all_responses[-1]:
                break
            all_responses.append(self.textract.get_document_analysis(
                JobId=job_id,
                NextToken=all_responses[-1]["NextToken"]
            ))
        return all_responses
