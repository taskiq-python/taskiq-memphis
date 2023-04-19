from typing import Any, Dict
from memphis import Headers
from memphis.types import Retention, Storage
from pydantic import BaseModel


class MemphisStorageParameters(BaseModel):
    """Parameters for memphis station."""

    name: str
    retention_type: Retention = Retention.MAX_MESSAGE_AGE_SECONDS
    retention_value: int = 604800
    storage_type: Storage = Storage.DISK
    replicas: int = 1
    idempotency_window_ms: int = 120000
    schema_name: str = ""
    send_poison_msg_to_dls: bool = True
    send_schema_failed_msg_to_dls: bool = True
    tiered_storage_enabled: bool = False


class MemphisProduceMethodParameters(BaseModel):
    """Parameters for memphis producer `produce` method."""

    ack_wait_sec: int = 15
    headers: Dict[Any, Any] = {}
    async_produce: bool = False


class MemphisProducerParameters(BaseModel):
    """Parameters for memphis producer."""

    producer_name: str
    generate_random_suffix: bool = False


class MemphisConsumerParameters(BaseModel):
    """Parameters for memphis consumer."""

    consumer_name: str
    consumer_group: str = ""
    pull_interval_ms: int = 1000
    batch_size: int = 10
    batch_max_time_to_wait_ms: int = 5000
    max_ack_time_ms: int = 30000
    max_msg_deliveries: int = 10
    generate_random_suffix: bool = False
    start_consume_from_sequence: int = 1
    last_messages: int = -1
