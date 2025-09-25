src/
  domain/                 # Núcleo de reglas de negocio
    models/               # Entidades, value objects
    services/             # Reglas puras de negocio
  application/            # Casos de uso
    use_cases/            # Ej: RunEtlUseCase
    ports/                # Interfaces: StoragePort, OcrPort, RepoPort
  presentation/           # Capa de entrada/salida
    controllers/          # FastAPI, gRPC, CLI
    dto/                  # Request/response models
  infrastructure/         # Implementaciones externas
    adapters/             # S3, Textract, Dynamo
    config/               # Settings, logging, retries