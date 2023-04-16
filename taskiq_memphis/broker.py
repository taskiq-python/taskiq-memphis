from typing import AsyncGenerator, Dict, Optional, Union
from taskiq import AsyncBroker, BrokerMessage
from memphis import Memphis, Headers
from memphis.consumer import Consumer
from memphis.producer import Producer
from memphis.station import Station
from memphis.types import Retention, Storage
from taskiq_memphis.exceptions import (
    StartupNotCalledError,
    TooLateConfigurationError,
)

from taskiq_memphis.models import (
    MemphisConsumerParameters,
    MemphisProduceMethodParameters,
    MemphisProducerParameters,
    MemphisStorageParameters,
)


class MemphisBroker(AsyncBroker):
    """Broker that works with Memphis."""

    def __init__(
        self,
        memphis_host: str,
        username: str,
        connection_token: str = "",
        password: str = "",
        port: int = 6666,
        reconnect: bool = True,
        max_reconnect: int = 10,
        reconnect_interval_ms: int = 1500,
        timeout_ms: int = 15000,
        cert_file: str = "",
        key_file: str = "",
        ca_file: str = "",
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
        :param station_name: name of the station.
        :param reconnect: turn on/off reconnection while connection is lost.
        :param max_reconnect: maximum reconnection attempts.
        :param reconnect_interval_ms: interval in milliseconds
            between reconnect attempts.
        :param timeout_ms: connection timeout in milliseconds.
        :param cert_file: path to tls cert file.
        :param key_file: path to tls key file.
        :param ca_file: path to tls ca file.

        :param destroy_station_on_shutdown: close station on shutdown.
        :param destroy_producer_on_shutdown: close producer on shutdown.
        :param destroy_consumer_on_shutdown: close consumer on shutdown.
        """
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

        self._destroy_station_on_shutdown = destroy_station_on_shutdown
        self._destroy_producer_on_shutdown = destroy_producer_on_shutdown
        self._destroy_consumer_on_shutdown = destroy_consumer_on_shutdown

        self._station_parameters: MemphisStorageParameters = (
            MemphisStorageParameters(
                name="taskiq",
            )
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

        self._is_producer_started: bool = False
        self._is_consumer_started: bool = False
        self._is_started: bool = False

        Memphis().consumer()

    def configure_station(
        self,
        name: str,
        retention_type: Retention = Retention.MAX_MESSAGE_AGE_SECONDS,
        retention_value: int = 604800,
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
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup."
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
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup."
            )

        self._producer_parameters = MemphisProducerParameters(
            producer_name=producer_name,
            generate_random_suffix=generate_random_suffix,
        )

    def configure_produce_method(
        self,
        generate_random_suffix: bool = False,
        ack_wait_sec: int = 15,
        headers: Optional[Headers] = None,
        async_produce: bool = False,
        is_idempotency_turned_on: bool = True,
    ) -> None:
        """Configure memphis producer `produce` method.

        In this method you can specify parameters for
        memphis producer `produce` method.
        Is will be used in kick method in your
        taskiq tasks.

        :param generate_random_suffix: generate random suffix or not.
        :param ack_wait_sec: wait ack time in seconds.
        :param headers: `Headers` instance from memphis.
        :param async_produce: produce message in async way or not.
        :param is_idempotency_turned_on: is idempotency turned on or not.
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup."
            )

        self._produce_method_parameters = MemphisProduceMethodParameters(
            generate_random_suffix=generate_random_suffix,
            ack_wait_sec=ack_wait_sec,
            headers=headers or Headers(),
            async_produce=async_produce,
            is_idempotency_turned_on=is_idempotency_turned_on,
        )

    def configure_consumer(
        self,
        consumer_name: str,
        consumer_group: str,
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
        """
        if self._is_started:
            raise TooLateConfigurationError(
                "Please call this method before startup."
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

        self._station = await self._memphis.station(
            **self._station_parameters.dict(),
        )

        if not self._is_producer_started:
            self._producer: Producer = await self._memphis.producer(  # type: ignore
                station_name=self._station_parameters.name,
                **self._producer_parameters.dict(),
            )
            self._is_producer_started = True

        if not self._is_consumer_started:
            self._consumer: Consumer = await self._memphis.consumer(  # type: ignore
                station_name=self._station_parameters.name,
                **self._consumer_parameters.dict(),
            )
            self._is_consumer_started = True

        self._is_started = True

    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        if self._destroy_station_on_shutdown and self._station:
            await self._station.destroy()

        if self._destroy_producer_on_shutdown and self._producer:
            await self._producer.destroy()

        if self._destroy_consumer_on_shutdown and self._consumer:
            await self._consumer.destroy()

        await self._memphis.close

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
        if not self._is_producer_started:
            raise StartupNotCalledError(
                "Please call startup method before kick tasks.",
            )

    async def listen(self) -> AsyncGenerator[bytes, None]:
        """Listen to new messages in the station.

        This function listens to the station and
            yields every new message.

        :yields: parsed broker message.
        :raises StartupNotCalledError: if startup wasn't called.
        """
        if not self._is_consumer_started:
            raise StartupNotCalledError(
                "Please call startup method before start listening.",
            )
        yield b"test"
