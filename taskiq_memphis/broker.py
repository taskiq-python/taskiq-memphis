import asyncio
import secrets
from logging import getLogger
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, TypeVar, Union

from memphis import Headers, Memphis
from memphis.consumer import Consumer
from memphis.exceptions import MemphisError
from memphis.message import Message
from memphis.producer import Producer
from memphis.station import Station
from memphis.types import Retention, Storage
from taskiq import AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_memphis.exceptions import StartupNotCalledError, TooLateConfigurationError
from taskiq_memphis.models import (
    MemphisConsumerParameters,
    MemphisProduceMethodParameters,
    MemphisProducerParameters,
    MemphisStorageParameters,
)

_T = TypeVar("_T")  # noqa: WPS111


logger = getLogger("taskiq.memphis_broker")


class MemphisBroker(AsyncBroker):
    """Broker that works with Memphis."""

    def __init__(  # noqa: S107, WPS211
        self,
        memphis_host: str,
        username: str,
        connection_token: str = "",
        password: str = "",
        port: int = 6666,
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        reconnect: bool = True,
        max_reconnect: int = 10,
        reconnect_interval_ms: int = 1500,
        timeout_ms: int = 15000,
        cert_file: str = "",
        key_file: str = "",
        ca_file: str = "",
        consumer_batch_size: int = 10,
        destroy_station_on_shutdown: bool = False,
        destroy_producer_on_shutdown: bool = True,
        destroy_consumer_on_shutdown: bool = True,
    ) -> None:
        """Construct a new broker.

        :param memphis_host: memphis host.
        :param username: user of type root/application.
        :param connection_token: connection token.
        :param password: password for memphis, default is connection
            token-based authentication
        :param port: port.
        :param result_backend: custom result backend.
        :param task_id_generator: custom task_id genertaor.
        :param reconnect: turn on/off reconnection while connection is lost.
        :param max_reconnect: maximum reconnection attempts.
        :param reconnect_interval_ms: interval in milliseconds
            between reconnect attempts.
        :param timeout_ms: connection timeout in milliseconds.
        :param cert_file: path to tls cert file.
        :param key_file: path to tls key file.
        :param ca_file: path to tls ca file.

        :param consumer_batch_size: batch size for the consumer.
        :param destroy_station_on_shutdown: close station on shutdown.
        :param destroy_producer_on_shutdown: close producer on shutdown.
        :param destroy_consumer_on_shutdown: close consumer on shutdown.
        """
        super().__init__(result_backend, task_id_generator)

        self._memphis_host: str = memphis_host
        self._username: str = username
        self._connection_token: str = connection_token
        self._password: str = password
        self._port: int = port
        self._reconnect: bool = reconnect
        self._max_reconnect: int = max_reconnect
        self._reconnect_interval_ms: int = reconnect_interval_ms
        self._timeout_ms: int = timeout_ms
        self._cert_file: str = cert_file
        self._key_file: str = key_file
        self._ca_file: str = ca_file

        self._consumer_batch_size = consumer_batch_size
        self._destroy_station_on_shutdown = destroy_station_on_shutdown
        self._destroy_producer_on_shutdown = destroy_producer_on_shutdown
        self._destroy_consumer_on_shutdown = destroy_consumer_on_shutdown

        self.messages_queue: asyncio.Queue[bytes] = asyncio.Queue()

        self._station_parameters: MemphisStorageParameters = MemphisStorageParameters(
            name="taskiq",
        )

        self._producer_parameters: MemphisProducerParameters = (
            MemphisProducerParameters(
                producer_name="taskiq_producer",
            )
        )

        self._produce_method_parameters: MemphisProduceMethodParameters = (
            MemphisProduceMethodParameters()
        )

        self._consumer_parameters: MemphisConsumerParameters = (
            MemphisConsumerParameters(consumer_name="taskiq_consumer")
        )

        self._memphis: Memphis = Memphis()
        self._station: Optional[Station] = None
        self._producer: Optional[Producer] = None
        self._consumer: Optional[Consumer] = None

        self._is_started: bool = False

    def configure_station(  # noqa: WPS211
        self,
        name: str,
        retention_type: Retention = Retention.MAX_MESSAGE_AGE_SECONDS,
        retention_value: int = 1,
        storage_type: Storage = Storage.DISK,
        replicas: int = 1,
        idempotency_window_ms: int = 120000,
        schema_name: str = "",
        send_poison_msg_to_dls: bool = True,
        send_schema_failed_msg_to_dls: bool = True,
        tiered_storage_enabled: bool = False,
    ) -> None:
        """Configure memphis station.

        In this method you can specify parameters for
        new memphis station.

        :param name: name of the station.
        :param retention_type: type of retention.
        :param retention_value: how long it takes to keep message,
            based on retention_type.
        :param storage_type: type of the storage.
        :param replicas: number of replicas.
        :param idempotency_window_ms: time frame in which idempotent
            messages will be tracked.
        :param schema_name: name of the schema.
        :param send_poison_msg_to_dls: send poisoned message to
            dead letter station or not.
        :param send_schema_failed_msg_to_dls: send schema failed message
            to dead letter station or not.
        :param tiered_storage_enabled: tiered storage enabled or not.

        :raises TooLateConfigurationError: if broker is already running.
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup.",
            )

        self._station_parameters = MemphisStorageParameters(
            name=name,
            retention_type=retention_type,
            retention_value=retention_value,
            storage_type=storage_type,
            replicas=replicas,
            idempotency_window_ms=idempotency_window_ms,
            schema_name=schema_name,
            send_poison_msg_to_dls=send_poison_msg_to_dls,
            send_schema_failed_msg_to_dls=send_schema_failed_msg_to_dls,
            tiered_storage_enabled=tiered_storage_enabled,
        )

    def configure_producer(
        self,
        producer_name: str,
        generate_random_suffix: bool = False,
    ) -> None:
        """Configure memphis producer.

        In this method you can specify parameters for
        memphis producer.
        Is will be used in kick method in your
        taskiq tasks.

        :param producer_name: name of the producer.
        :param generate_random_suffix: generate random suffix or not.

        :raises TooLateConfigurationError: if broker is already running.
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup.",
            )

        self._producer_parameters = MemphisProducerParameters(
            producer_name=producer_name,
            generate_random_suffix=generate_random_suffix,
        )

    def configure_produce_method(
        self,
        ack_wait_sec: int = 15,
        headers: Optional[Headers] = None,
        async_produce: bool = False,
    ) -> None:
        """Configure memphis producer `produce` method.

        In this method you can specify parameters for
        memphis producer `produce` method.
        Is will be used in kick method in your
        taskiq tasks.

        :param ack_wait_sec: wait ack time in seconds.
        :param headers: `Headers` instance from memphis.
        :param async_produce: produce message in async way or not.

        :raises TooLateConfigurationError: if broker is already running.
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup.",
            )

        self._produce_method_parameters = MemphisProduceMethodParameters(
            ack_wait_sec=ack_wait_sec,
            headers=headers or Headers(),
            async_produce=async_produce,
        )

    def configure_consumer(  # noqa: WPS211
        self,
        consumer_name: str,
        consumer_group: str = "",
        pull_interval_ms: int = 1000,
        batch_size: int = 10,
        batch_max_time_to_wait_ms: int = 5000,
        max_ack_time_ms: int = 30000,
        max_msg_deliveries: int = 10,
        generate_random_suffix: bool = False,
        start_consume_from_sequence: int = 1,
        last_messages: int = -1,
    ) -> None:
        """Configure memphis consumer.

        In this method you can specify any parameter for
        memphis consumer.
        Is will be used in listen method in your
        taskiq worker.

        :param consumer_name: consumer name.
        :param consumer_group: name of the consumer group.
        :param pull_interval_ms: interval in milliseconds between pulls.
        :param batch_size: pull batch size.
        :param batch_max_time_to_wait_ms: max time in milliseconds
            to wait between pulls.
        :param max_ack_time_ms: max time for ack a message in milliseconds.
        :param max_msg_deliveries: max number of message deliveries.
        :param generate_random_suffix: concatenate a random suffix to consumer's name.
        :param start_consume_from_sequence: start consuming from a specific sequence.
        :param last_messages: consume the last N messages.

        :raises TooLateConfigurationError: if broker is already running.
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup.",
            )

        self._consumer_parameters = MemphisConsumerParameters(
            consumer_name=consumer_name,
            consumer_group=consumer_group,
            pull_interval_ms=pull_interval_ms,
            batch_size=batch_size,
            batch_max_time_to_wait_ms=batch_max_time_to_wait_ms,
            max_ack_time_ms=max_ack_time_ms,
            max_msg_deliveries=max_msg_deliveries,
            generate_random_suffix=generate_random_suffix,
            start_consume_from_sequence=start_consume_from_sequence,
            last_messages=last_messages,
        )

    async def startup(self) -> None:
        """Create memphis instance, station, consumer and producer."""
        await self._memphis.connect(
            host=self._memphis_host,
            username=self._username,
            connection_token=self._connection_token,
            password=self._password,
            port=self._port,
            reconnect=self._reconnect,
            max_reconnect=self._max_reconnect,
            reconnect_interval_ms=self._reconnect_interval_ms,
            timeout_ms=self._timeout_ms,
            cert_file=self._cert_file,
            key_file=self._key_file,
            ca_file=self._ca_file,
        )

        try:
            self._station = await self._memphis.station(
                **self._station_parameters.dict(),
            )
        except MemphisError as exc:
            logger.warning(
                f"Can't create station with "  # noqa: WPS237
                f"name {self._station_parameters.name} "
                f"due to {exc}",
            )

        if not self._producer and not self.is_worker_process:
            self._producer: Producer = await self._memphis.producer(  # type: ignore
                station_name=self._station_parameters.name,
                **self._producer_parameters.dict(),
            )

        if not self._consumer and self.is_worker_process:
            consumer_parameters = self._consumer_parameters.dict()

            # It is necessary because memphis library can't destroy
            # consumers if consumers were created using inner
            # memphis functionality.
            generate_random_suffix = consumer_parameters.pop(
                "generate_random_suffix",
            )
            consumer_name = consumer_parameters.pop("consumer_name")
            if generate_random_suffix:
                consumer_name = "{0}_{1}".format(
                    consumer_name,
                    secrets.token_urlsafe(8).lower(),
                )

            self._consumer: Consumer = await self._memphis.consumer(  # type: ignore
                station_name=self._station_parameters.name,
                consumer_name=consumer_name,
                **consumer_parameters,
            )

        self._is_started = True

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        if self._destroy_producer_on_shutdown and self._producer:
            await self._producer.destroy()

        if self._destroy_consumer_on_shutdown and self._consumer:
            await self._consumer.destroy()

        if self._destroy_station_on_shutdown and self._station:
            await self._station.destroy()

        await self._memphis.close()

    async def kick(self, message: BrokerMessage) -> None:
        """Send message to the station.

        This method constructs message for memphis station
        and sends it.

        The message has task_id and task_name and labels
        in headers. And message's routing key is the same
        as the task_name.

        :raises StartupNotCalledError: if startup wasn't called.
        :param message: message to send.
        """
        if not self._producer:
            raise StartupNotCalledError(
                "Please call startup method before kick tasks.",
            )

        await self._producer.produce(
            message=bytearray(message.message),
            **self._produce_method_parameters.dict(),
        )

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Listen to new messages in the station.

        This function listens to the station and
            yields every new message.

        :yields: parsed broker message.
        :raises StartupNotCalledError: if startup wasn't called.
        """
        if not self._consumer:
            raise StartupNotCalledError(
                "Please call startup method before start listening.",
            )

        # consume method creates background task with asyncio.create_task
        self._consumer.consume(self.process_memphis_messages)

        while True:  # noqa: WPS457
            yield await self.messages_queue.get()

    async def process_memphis_messages(
        self,
        memphis_messages: Optional[List[Message]],
        *args: Union[Exception, Dict[Any, Any]],
    ) -> None:
        """Process messages from memphis station.

        Convert bytearray to bytes and put message into the queue.

        :param memphis_messages: messages from memphis.
        :param args: additional arguments from memphis.
        """
        if not memphis_messages:
            return

        for memphis_message in memphis_messages:
            bytearray_message: bytearray = memphis_message.get_data()
            await self.messages_queue.put(bytes(bytearray_message))
            await memphis_message.ack()
