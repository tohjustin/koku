#
# Copyright 2019 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""Sources Integration Service."""
import itertools
import logging
import queue
import sys
import threading
import time
from xmlrpc.server import SimpleXMLRPCServer

from confluent_kafka import Consumer
from confluent_kafka import TopicPartition
from django.db import connections
from django.db import DEFAULT_DB_ALIAS
from django.db import IntegrityError
from django.db import InterfaceError
from django.db import OperationalError
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver
from rest_framework.exceptions import ValidationError

from api.provider.models import Sources
from kafka_utils.utils import backoff
from kafka_utils.utils import is_kafka_connected
from masu.prometheus_stats import KAFKA_CONNECTION_ERRORS_COUNTER
from sources import storage
from sources.config import Config
from sources.kafka_message_processor import create_msg_processor
from sources.kafka_message_processor import SourcesMessageError
from sources.sources_http_client import SourceNotFoundError
from sources.sources_http_client import SourcesHTTPClient
from sources.sources_http_client import SourcesHTTPClientError
from sources.sources_patch_handler import SourcesPatchHandler
from sources.sources_provider_coordinator import select_coordinator
from sources.sources_provider_coordinator import SourcesProviderCoordinatorError

LOG = logging.getLogger(__name__)

PROCESS_QUEUE = queue.PriorityQueue()
COUNT = itertools.count()  # next(COUNT) returns next sequential number


class SourcesIntegrationError(ValidationError):
    """Sources Integration error."""


def _collect_pending_items():
    """Gather all sources to create update, or delete."""
    create_events = storage.load_providers_to_create()
    update_events = storage.load_providers_to_update()
    destroy_events = storage.load_providers_to_delete()
    return create_events + update_events + destroy_events


def _log_process_queue_event(queue, event):
    """Log process queue event."""
    operation = event.get("operation", "unknown")
    source = event.get("source")
    name = source.name if source else "unknown"
    LOG.info(f"Adding operation {operation} for {name} to process queue (size: {queue.qsize()})")


def close_and_set_db_connection():  # pragma: no cover
    """Close the db connection and set to None."""
    connections[DEFAULT_DB_ALIAS].connection.close()
    connections[DEFAULT_DB_ALIAS].connection = None


def load_process_queue():
    """
    Re-populate the process queue for any Source events that need synchronization.

    Handles the case for when the Sources Integration service goes down before
    Koku Synchronization could be completed.

    Args:
        None

    Returns:
        None

    """
    pending_events = _collect_pending_items()
    for event in pending_events:
        _log_process_queue_event(PROCESS_QUEUE, event)
        PROCESS_QUEUE.put_nowait((next(COUNT), event))


def execute_process_queue():
    """Execute process queue to synchronize providers."""
    while not PROCESS_QUEUE.empty():
        msg_tuple = PROCESS_QUEUE.get()
        process_synchronize_sources_msg(msg_tuple, PROCESS_QUEUE)


@receiver(post_save, sender=Sources)
def storage_callback(sender, instance, **kwargs):
    """Load Sources ready for Koku Synchronization when Sources table is updated."""
    if instance.koku_uuid and instance.pending_update and not instance.pending_delete:
        update_event = {"operation": "update", "source": instance}
        _log_process_queue_event(PROCESS_QUEUE, update_event)
        LOG.debug(f"Update Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), update_event))

    if instance.pending_delete:
        delete_event = {"operation": "destroy", "source": instance}
        _log_process_queue_event(PROCESS_QUEUE, delete_event)
        LOG.debug(f"Delete Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), delete_event))

    process_event = storage.screen_and_build_provider_sync_create_event(instance)
    if process_event:
        _log_process_queue_event(PROCESS_QUEUE, process_event)
        LOG.debug(f"Create Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), process_event))

    execute_process_queue()


def get_consumer():
    """Create a Kafka consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": Config.SOURCES_KAFKA_ADDRESS,
            "group.id": "hccm-sources",
            "queued.max.messages.kbytes": 1024,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([Config.SOURCES_TOPIC])
    return consumer


def listen_for_messages_loop(application_source_id):  # pragma: no cover
    """Wrap listen_for_messages in while true."""
    consumer = get_consumer()
    LOG.info("Consumer is listening for messages...")
    while True:  # equivalent to while True, but mockable
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            KAFKA_CONNECTION_ERRORS_COUNTER.inc()
            LOG.warning(f"[listen_for_messages_loop] consumer.poll message: {msg}. Error: {msg.error()}")
            continue

        listen_for_messages(msg, consumer, application_source_id)
        execute_process_queue()


def rewind_consumer_to_retry(consumer, topic_partition):
    """Helper method to log and rewind kafka consumer for retry."""
    LOG.info(f"Seeking back to offset: {topic_partition.offset}, partition: {topic_partition.partition}")
    consumer.seek(topic_partition)
    time.sleep(Config.RETRY_SECONDS)


@KAFKA_CONNECTION_ERRORS_COUNTER.count_exceptions()  # noqa: C901
def listen_for_messages(kaf_msg, consumer, application_source_id):  # noqa: C901
    """
    Listen for Platform-Sources kafka messages.

    Args:
        consumer (Consumer): Kafka consumer object
        application_source_id (Integer): Cost Management's current Application Source ID. Used for
            kafka message filtering.

    Returns:
        None

    """
    try:
        msg_processor = create_msg_processor(kaf_msg, application_source_id)
        if msg_processor and msg_processor.source_id:
            tp = TopicPartition(Config.SOURCES_TOPIC, msg_processor.partition, msg_processor.offset)
            LOG.info(f"Processing message offset: {msg_processor.offset} partition: {msg_processor.partition}")
            LOG.info(f"Cost Management Message to process: {msg_processor}")
            with transaction.atomic():
                msg_processor.process()
    except (InterfaceError, OperationalError) as error:
        close_and_set_db_connection()
        LOG.error(f"[listen_for_messages] Database error. Error: {type(error).__name__}: {error}. Retrying...")
        # time.sleep(Config.RETRY_SECONDS)
        rewind_consumer_to_retry(consumer, tp)
    except (IntegrityError, SourcesHTTPClientError) as error:
        LOG.error(f"[listen_for_messages] Internal error. {type(error).__name__}: {error}. Retrying...", exc_info=True)
        # time.sleep(Config.RETRY_SECONDS)
        rewind_consumer_to_retry(consumer, tp)
    except (SourcesMessageError, SourceNotFoundError) as error:
        LOG.warning(f"[listen_for_messages] {type(error).__name__}: {error}. Skipping msg: {kaf_msg}")
        consumer.store_offsets(kaf_msg)
        consumer.commit()
    except Exception as error:
        LOG.error(f"[listen_for_messages] UNKNOWN error encountered: {type(error).__name__}: {error}", exc_info=True)
    else:
        consumer.store_offsets(kaf_msg)
        consumer.commit()


def execute_koku_provider_op(msg):
    """
    Execute the 'create' or 'destroy Koku-Provider operations.

    'create' operations:
        Koku POST /providers is executed along with updating the Sources database table with
        the Koku Provider uuid.
    'destroy' operations:
        Koku DELETE /providers is executed along with removing the Sources database entry.

    Two types of exceptions are handled for Koku HTTP operations.  Recoverable client and
    Non-Recoverable client errors.  If the error is recoverable the calling function
    (synchronize_sources) will re-queue the operation.

    Args:
        msg (Asyncio msg): Dictionary messages containing operation,
                                       provider and offset.
            example: {'operation': 'create', 'provider': SourcesModelObj, 'offset': 3}

    Returns:
        None

    """
    sources_obj = msg.get("source")
    operation = msg.get("operation")
    account_coordinator = select_coordinator(sources_obj, operation)
    sources_client = SourcesHTTPClient(sources_obj.auth_header, sources_obj.source_id)

    try:
        if account_coordinator:
            account_coordinator.perform_operation()
        sources_client.set_source_status(None)
    except SourcesProviderCoordinatorError as account_error:
        raise SourcesIntegrationError(f"Koku provider error: {account_error}")
    except ValidationError as account_error:
        err_msg = f"Unable to {operation} provider for Source ID: {sources_obj.source_id}. Reason: {account_error}"
        LOG.error(err_msg)
        sources_client.set_source_status(account_error)


def _requeue_provider_sync_message(priority, msg, queue):
    """Helper to requeue provider sync messages."""
    time.sleep(Config.RETRY_SECONDS)
    _log_process_queue_event(queue, msg)
    queue.put((priority, msg))
    LOG.warning(
        f'Requeue of failed operation: {msg.get("operation")} '
        f'for Source ID: {str(msg.get("provider").source_id)} complete.'
    )


def process_synchronize_sources_msg(msg_tuple, process_queue):
    """
    Synchronize Platform Sources with Koku Providers.

    Task will process the process_queue which contains filtered
    events (Cost Management Platform-Sources).

    The items on the queue are Koku-Provider 'create' or 'destroy
    events.  If the Koku-Provider operation fails the event will
    be re-queued until the operation is successful.

    Args:
        process_queue (Asyncio.Queue): Dictionary messages containing operation,
                                       provider and offset.
            example: {'operation': 'create', 'provider': SourcesModelObj, 'offset': 3}

    Returns:
        None

    """
    priority, msg = msg_tuple

    LOG.info(
        f'Koku provider operation to execute: {msg.get("operation")} '
        f'for Source ID: {str(msg.get("source").source_id)}'
    )
    try:
        execute_koku_provider_op(msg)
        LOG.info(
            f'Koku provider operation to execute: {msg.get("operation")} '
            f'for Source ID: {str(msg.get("source").source_id)} complete.'
        )
        if msg.get("operation") != "destroy":
            storage.clear_update_flag(msg.get("source").source_id)

    except (IntegrityError, SourcesIntegrationError) as error:
        LOG.warning(f"[synchronize_sources] Re-queuing failed operation. Error: {error}")
        _requeue_provider_sync_message(priority, msg, process_queue)
    except (InterfaceError, OperationalError) as error:
        close_and_set_db_connection()
        LOG.warning(
            f"[synchronize_sources] Closing DB connection and re-queueing failed operation."
            f" Encountered {type(error).__name__}: {error}"
        )
        _requeue_provider_sync_message(priority, msg, process_queue)
    except Exception as error:
        # The reason for catching all exceptions is to ensure that the event
        # loop remains active in the event that provider synchronization fails unexpectedly.
        source = msg.get("source")
        source_id = source.source_id if source else "unknown"
        LOG.error(
            f"[synchronize_sources] Unexpected synchronization error for Source ID {source_id} "
            f"encountered: {type(error).__name__}: {error}",
            exc_info=True,
        )


@KAFKA_CONNECTION_ERRORS_COUNTER.count_exceptions()
def sources_integration_thread():  # pragma: no cover
    """
    Configure Sources listener thread.

    Returns:
        None

    """
    cost_management_type_id = None
    count = 0
    while cost_management_type_id is None:
        # First, hit Souces endpoint to get the cost-mgmt application ID.
        # Without this initial connection/ID number, the consumer cannot start
        try:
            cost_management_type_id = SourcesHTTPClient(
                Config.SOURCES_FAKE_HEADER
            ).get_cost_management_application_type_id()
            LOG.info("Connected to Sources REST API.")
        except SourcesHTTPClientError as error:
            LOG.error(f"Unable to connect to Sources REST API. Error: {error}")
            backoff(count)
            count += 1
            LOG.info("Reattempting connection to Sources REST API.")
        except SourceNotFoundError as err:
            LOG.error(f"Cost Management application not found: {err}. Exiting...")
            sys.exit(1)
        except KeyboardInterrupt:
            sys.exit(0)

    # Next, check that Kafka is running
    if is_kafka_connected(Config.SOURCES_KAFKA_HOST, Config.SOURCES_KAFKA_PORT):
        LOG.info("Kafka is running...")

    load_process_queue()
    execute_process_queue()

    listen_for_messages_loop(cost_management_type_id)


def rpc_thread():
    """RPC Server to serve PATCH requests."""
    LOG.info(f"Starting RPC server. Port: {Config.SOURCES_CLIENT_RPC_PORT}")
    with SimpleXMLRPCServer(("0.0.0.0", Config.SOURCES_CLIENT_RPC_PORT), allow_none=True) as server:
        server.register_introspection_functions()
        server.register_instance(SourcesPatchHandler())
        server.serve_forever()


def initialize_sources_integration():  # pragma: no cover
    """Start Sources integration thread."""

    event_loop_thread = threading.Thread(target=sources_integration_thread)
    event_loop_thread.start()
    LOG.info("Listening for kafka events")
    rpc = threading.Thread(target=rpc_thread)
    rpc.start()
