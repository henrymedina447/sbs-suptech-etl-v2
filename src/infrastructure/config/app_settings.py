import os
from functools import lru_cache
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

path_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
dotenv_path = os.path.join(path_root, ".env")
load_dotenv(dotenv_path, override=True)


class AwsSettings(BaseModel):
    access_key_id: str = Field(
        description="es el access key de la cuenta obtenido en el IAM"
    )
    secret: str = Field(description="es el secret key de la cuenta obtenido en el IAM")
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
            )
        except (KeyError, ValidationError) as e:
            raise RuntimeError(f"Configuración invalidad: {e}") from e


@lru_cache(maxsize=1)
def get_app_settings() -> AppSettings:
    return AppSettings.load()
