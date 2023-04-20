import asyncio
import uuid

import pytest
from memphis.producer import Producer
from taskiq import BrokerMessage

from taskiq_memphis.broker import MemphisBroker


async def get_first_task(broker: MemphisBroker) -> bytes:  # type: ignore
    """
    Get first message from the queue.

    :param broker: async message broker.
    :return: first message from listen method
    """
    async for message in broker.listen():
        return message


@pytest.mark.anyio
async def test_kick_success(broker: MemphisBroker) -> None:
    """Test that message is published in the station and read correctly.

    We kick the message and then try to listen to the station,
    and check that message we got is the same as we sent.

    :param broker: memphis broker.
    """
    task_id = uuid.uuid4().hex
    task_name = uuid.uuid4().hex

    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )

    await broker.kick(sent)

    message = await asyncio.wait_for(get_first_task(broker), timeout=15)

    assert message == sent.message


@pytest.mark.anyio
async def test_startup(broker_without_startup: MemphisBroker) -> None:
    """Check that startup method works correctly.

    :param broker_without_startup: memphis broker without called startup.
    """
    broker_without_startup.is_worker_process = True
    await broker_without_startup.startup()
    assert broker_without_startup._consumer

    # Need because memphis library has a mistake
    # So without this line we will receive an error.
    broker_without_startup._consumer.t_consume = None

    broker_without_startup.is_worker_process = False
    await broker_without_startup.startup()
    assert broker_without_startup._producer

    assert broker_without_startup._station


@pytest.mark.anyio
async def test_listen(
    broker_without_startup: MemphisBroker,
    memphis_producer: Producer,
) -> None:
    """Test that message are read correctly.

    Tests that broker listens to the queue
    correctly and listen can be iterated.

    :param broker_without_startup: memphis broker.
    :param memphis_producer: producer.
    """
    broker_without_startup.is_worker_process = True
    await broker_without_startup.startup()

    message_to_send = b"my_msg"
    await memphis_producer.produce(
        message=bytearray(message_to_send),
    )

    received_message = await asyncio.wait_for(
        get_first_task(broker_without_startup),
        timeout=15,
    )

    assert message_to_send == received_message
