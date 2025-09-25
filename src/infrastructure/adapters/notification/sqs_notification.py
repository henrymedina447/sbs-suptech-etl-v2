import boto3
import json

from application.ports.notification_port import NotificationPort
from mypy_boto3_sqs import SQSClient

from botocore.config import Config
from infrastructure.config.app_settings import AppSettings, get_app_settings

from domain.models.notification import Notification


class SqsNotification(NotificationPort):
    def __init__(self):
        self.app_settings: AppSettings = get_app_settings()
        self.queue: SQSClient = self._get_configuration()

    def _get_configuration(self) -> SQSClient:
        _cfg = Config(
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=3,
            read_timeout=5,
        )
        sqs_client: SQSClient = boto3.client(
            "sqs", config=_cfg, region_name=self.app_settings.aws_settings.region
        )
        return sqs_client

    def notify(self, notifications: list[Notification]):
        sqs_messages = [
            {"Id": notification.id, "MessageBody": notification.message.model_dump_json(by_alias=True)}
            for notification in notifications
        ]

        print("sqs_messages", sqs_messages)
        print("queue_url", self.app_settings.sqs_settings.queue_url)
        self.queue.send_message_batch(
            QueueUrl=self.app_settings.sqs_settings.queue_url, Entries=sqs_messages
        )
