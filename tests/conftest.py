import os
from typing import AsyncGenerator
from uuid import uuid4
import pytest

from memphis import Memphis
from memphis.consumer import Consumer
from memphis.producer import Producer
from memphis.station import Station

from taskiq_memphis.broker import MemphisBroker


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """Backend for anyio pytest plugin.

    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def memphis_host() -> str:
    """Get custom memphis host.

    This function tries to get custom memphis host,
    or returns default otherwise.

    :return: rabbitmq url.
    """
    return os.environ.get("TEST_MEMPHIS_HOST", "localhost")


@pytest.fixture
def station_name() -> str:
    """Generate new station name."""
    return uuid4().hex


@pytest.fixture
def producer_name() -> str:
    """Generate new producer name."""
    return uuid4().hex


@pytest.fixture
def consumer_name() -> str:
    """Generate new consumer name."""
    return uuid4().hex


@pytest.fixture
async def memphis(memphis_host: str) -> Memphis:
    """Create new memphis instance.

    :param memphis_host: host to memphis.

    :returns: new connected memphis instance.
    """
    memphis = Memphis()
    await memphis.connect(
        host=memphis_host,
        username="root",
        password="memphis",
    )
    return memphis


@pytest.fixture
async def memphis_station(
    memphis: Memphis,
    station_name: str,
) -> Station:
    """Create memphis station.

    :param memphis: connected memphis instance.
    :param station_name: name of the station.

    :returns: new station.
    """
    return await memphis.station(name=station_name)


@pytest.fixture
async def memphis_producer(
    memphis: Memphis,
    station_name: str,
    producer_name: str,
) -> Producer:
    """Create memphis consumer.

    :param memphis: connected memphis instance.
    :param station_name: name of the station.
    :param producer_name: name of the producer.

    :returns: new producer.
    """
    return await memphis.producer(
        station_name=station_name,
        producer_name=producer_name,
    )


@pytest.fixture
async def memphis_consumer(
    memphis: Memphis,
    station_name: str,
    consumer_name: str,
) -> Consumer:
    """Create memphis consumer.

    :param memphis: connected memphis instance.
    :param station_name: name of the station.
    :param consumer_name: name of the producer.

    :returns: new producer.
    """
    return await memphis.consumer(
        station_name=station_name,
        consumer_name=consumer_name,
    )


@pytest.fixture
async def broker(
    memphis_host: str,
    station_name: str,
    producer_name: str,
    consumer_name: str,
) -> AsyncGenerator[MemphisBroker, None]:
    """Create new memphis broker.

    :param memphis_host: host to memphis.
    :param station_name: name of the station.
    :param producer_name: name of the producer.
    :param consumer_name: name of the consumer.

    :yields: started MemphisBroker.
    """
    broker = MemphisBroker(
        memphis_host=memphis_host,
        username="root",
        password="memphis",
        destroy_station_on_shutdown=True,
    )

    broker.configure_station(
        name=station_name,
    )
    broker.configure_producer(
        producer_name=producer_name,
    )
    broker.configure_consumer(
        consumer_name=consumer_name,
    )

    broker.is_worker_process = True
    await broker.startup()
    broker.is_worker_process = False
    await broker.startup()

    yield broker

    await broker.shutdown()


@pytest.fixture
async def broker_without_startup(
    memphis_host: str,
    station_name: str,
    producer_name: str,
    consumer_name: str,
) -> AsyncGenerator[MemphisBroker, None]:
    """Create new memphis broker but don't call startup method.

    :param memphis_host: host to memphis.
    :param station_name: name of the station.
    :param producer_name: name of the producer.
    :param consumer_name: name of the consumer.

    :yields: started MemphisBroker.
    """
    broker = MemphisBroker(
        memphis_host=memphis_host,
        username="root",
        password="memphis",
        destroy_station_on_shutdown=True,
    )
    broker.configure_station(
        name=station_name,
    )
    broker.configure_producer(
        producer_name=producer_name,
    )
    broker.configure_consumer(
        consumer_name=consumer_name,
    )

    yield broker

    await broker.shutdown()
