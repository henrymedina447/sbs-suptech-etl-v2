import os
from functools import lru_cache
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

path_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
dotenv_path = os.path.join(path_root, ".env")
load_dotenv(dotenv_path, override=True)


class KafkaSettings(BaseModel):
    bootstrap_servers: str = Field(description="")
    topic: str = Field(description="Tópico del mensaje a escuchar")
    group_id: str = Field(
        description="Es el ID que identifica quien lo está consumiendo"
    )
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = (
        Field(description="Indica el protocolo de seguridad", default="PLAINTEXT")
    )
    sasl_mechanism: str | None = Field(description="", default=None)
    sasl_username: str | None = Field(description="", default=None)
    sasl_password: str | None = Field(description="", default=None)


class AwsSettings(BaseModel):
    access_key_id: str | None = Field(
        description="es el access key de la cuenta obtenido en el IAM"
    )
    secret: str | None= Field(description="es el secret key de la cuenta obtenido en el IAM")
    region: str = Field(description="La región de la aplicación")


class S3Settings(BaseModel):
    bucket: str = Field(default="Nombre del bucket")
    bucket_origin: str = Field(default="origin")
    bucket_destiny: str = Field(default="processed")


class TableSettings(BaseModel):
    si_table: str = Field(description="Tabla de supervised items en dynamo")


class SqsSettings(BaseModel):
    queue_url: str = Field(description="URL de la queue SQS")


class AppSettings(BaseModel):
    aws_settings: AwsSettings = Field(description="Todas las configuraciones de AWS")
    s3_settings: S3Settings = Field(
        description="Todas las configuraciones asociadas al bucket s3 de obtener los documentos a procesar"
    )
    table_settings: TableSettings = Field(
        description="Todas las configuraciones de las tablas"
    )
    sqs_settings: SqsSettings = Field(
        description="Todas las configuraciones de las tablas"
    )
    kafka_settings: KafkaSettings = Field(
        description="Todas las configuraciones asociadas al kafka"
    )

    @classmethod
    def load(cls) -> "AppSettings":
        try:
            return cls(
                aws_settings=AwsSettings(
                    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
                    region=os.getenv("AWS_DEFAULT_REGION"),
                ),
                s3_settings=S3Settings(
                    bucket=os.getenv("BUCKET_NAME"),
                    bucket_origin="origin",
                    bucket_destiny="processed",
                ),
                table_settings=TableSettings(
                    si_table=os.getenv("SUPERVISED_ITEMS_TABLE"),
                ),
                sqs_settings=SqsSettings(
                    queue_url=os.getenv("NOTIFICATION_QUEUE_URL"),
                ),
                kafka_settings=KafkaSettings(
                    bootstrap_servers=os.getenv("AWS_KAFKA_BOOTSTRAP_SERVERS"),
                    topic=os.getenv("AWS_KAFKA_TOPIC"),
                    group_id=os.getenv("AWS_KAFKA_GROUP_ID"),
                    security_protocol="PLAINTEXT",
                    sasl_mechanism=None,
                    sasl_username=None,
                    sasl_password=None,
                ),
            )
        except (KeyError, ValidationError) as e:
            raise RuntimeError(f"Configuración invalidad: {e}") from e


@lru_cache(maxsize=1)
def get_app_settings() -> AppSettings:
    return AppSettings.load()
