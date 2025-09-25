from pydantic import BaseModel, Field


class DocumentContractState(BaseModel):
    key: str = Field(description="El key del archivo")
    folder: str = Field(description="El nombre del folder donde se ubica el archivo")
    document_name: str = Field(description="El nombre del archivo a procesar")
    period_month: str | None = Field(description="El més de donde se obtiene el archivo", default=None)
    period_year: str | None = Field(description="El año de donde se obtiene el archivo", default=None)
