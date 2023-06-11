# taskiq-memphis

This library provides you with memphis broker for taskiq.
**You need python version >=3.8**

### Usage:
```python
from taskiq_memphis import MemphisBroker

broker = MemphisBroker(
    memphis_host="localhost",
    username="root",
    password="memphis",
)


@broker.task
async def the_best_task_ever() -> None:
    print("The best task ever")
```

## Configuration

MemphisBroker parameters:
* `memphis_host` - host to memphis.
* `port` - memphis server port.
* `username` - username.
* `connection_token` - connection token.
* `password` - password for username.
* `result_backend` - custom result backend.
* `task_id_generator` - custom task_id genertaor.
* `reconnect` - turn on/off reconnection while connection is lost.
* `max_reconnect` - maximum reconnection attempts.
* `reconnect_interval_ms` - interval in milliseconds between reconnect attempts.
* `timeout_ms` - connection timeout in milliseconds.
* `cert_file` - path to tls cert file.
* `key_file` - path to tls key file.
* `ca_file` - path to tls ca file.
* `consumer_batch_size` - batch size for the consumer.
* `destroy_station_on_shutdown` - close station on shutdown.
* `destroy_producer_on_shutdown` - close producer on shutdown.
* `destroy_consumer_on_shutdown` - close consumer on shutdown.


## Non-obvious things

You can configure memphis topic, consumer, producer and `produce` method with:
```python
# Create broker
broker = MemphisBroker(
    memphis_host="localhost",
    username="root",
    password="memphis",
    destroy_station_on_shutdown=True,
)

# Configure station
broker.configure_station(...)

# Configure producer
broker.configure_producer(...)

# Configure produce method
broker.configure_produce_method(...)

# Configure consumer
broker.configure_consumer(...)
```

Memphis `station` parameters you can configure:
* `name` - name of the station. Required.
* `retention_type` - type of message retention.
* `retention_value` - how long it takes to keep message, based on retention_type.
* `storage_type` - type of the storage, DISK or MEMORY.
* `replicas` - number of replicas.
* `idempotency_window_ms` - time frame in which idempotent messages will be tracked.
* `schema_name` - name of the schema. (You can create it only via memphis UI now)
* `send_poison_msg_to_dls` - send poisoned message to dead letter station or not.
* `send_schema_failed_msg_to_dls` - send schema failed message to dead letter station or not.
* `tiered_storage_enabled` - tiered storage enabled or not.

Memphis `producer` parameters you can configure:
* `producer_name` - producer name. Required.
* `generate_random_suffix` - add suffix to producer name. Default - `True`.
**DON'T SET THIS VARIABLE TO `FALSE` IF YOU WANT TO USE MORE THAN ONE PRODUCER.**

Memphis `produce` method parameters you can configure:
* `ack_wait_sec` - wait ack time in seconds.
* `headers` - `Headers` instance from memphis.
* `async_produce` - produce message in async way or not.

Memphis `consumer` parameters you can configure:

* `consumer_name` - name of the consumer. Required.
* `consumer_group` - name of the consumer group.
* `pull_interval_ms` - interval in milliseconds between pulls.
* `batch_size` - pull batch size.
* `batch_max_time_to_wait_ms` - max time in milliseconds to wait between pulls.
* `max_ack_time_ms` - max time for ack a message in milliseconds.
* `max_msg_deliveries` - max number of message deliveries.
* `generate_random_suffix` - concatenate a random suffix to consumer's name.
**DON'T SET THIS VARIABLE TO `FALSE` IF YOU WANT TO USE MORE THAN ONE CONSUMER.**
* `start_consume_from_sequence` - start consuming from a specific sequence.
* `last_messages` - consume the last N messages.
