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
import random
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
from kafka.errors import KafkaError
from rest_framework.exceptions import ValidationError

from api.provider.models import Provider
from api.provider.models import Sources
from masu.prometheus_stats import KAFKA_CONNECTION_ERRORS_COUNTER
from sources import storage
from sources.api.status import check_kafka_connection
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
SOURCES_OCP_SOURCE_NAME = "openshift"
SOURCES_AWS_SOURCE_NAME = "amazon"
SOURCES_AWS_LOCAL_SOURCE_NAME = "amazon-local"
SOURCES_AZURE_SOURCE_NAME = "azure"
SOURCES_AZURE_LOCAL_SOURCE_NAME = "azure-local"

SOURCE_PROVIDER_MAP = {
    SOURCES_OCP_SOURCE_NAME: Provider.PROVIDER_OCP,
    SOURCES_AWS_SOURCE_NAME: Provider.PROVIDER_AWS,
    SOURCES_AWS_LOCAL_SOURCE_NAME: Provider.PROVIDER_AWS_LOCAL,
    SOURCES_AZURE_SOURCE_NAME: Provider.PROVIDER_AZURE,
    SOURCES_AZURE_LOCAL_SOURCE_NAME: Provider.PROVIDER_AZURE_LOCAL,
}


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
    provider = event.get("provider")
    name = provider.name if provider else "unknown"
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
        update_event = {"operation": "update", "provider": instance}
        _log_process_queue_event(PROCESS_QUEUE, update_event)
        LOG.debug(f"Update Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), update_event))

    if instance.pending_delete:
        delete_event = {"operation": "destroy", "provider": instance}
        _log_process_queue_event(PROCESS_QUEUE, delete_event)
        LOG.debug(f"Delete Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), delete_event))

    process_event = storage.screen_and_build_provider_sync_create_event(instance)
    if process_event:
        _log_process_queue_event(PROCESS_QUEUE, process_event)
        LOG.debug(f"Create Event Queued for:\n{str(instance)}")
        PROCESS_QUEUE.put_nowait((next(COUNT), process_event))

    execute_process_queue()


def save_auth_info(auth_header, source_id):
    """
    Store Sources Authentication information given an Source ID.

    This method is called when a Cost Management application is
    attached to a given Source as well as when an Authentication
    is created.  We have to handle both cases since an
    Authentication.create event can occur before a Source is
    attached to the Cost Management application.

    Authentication is stored in the Sources database table.

    Args:
        source_id (Integer): Platform Sources ID.
        auth_header (String): Authentication Header.

    Returns:
        None

    """
    source_type = storage.get_source_type(source_id)

    if source_type:
        sources_network = SourcesHTTPClient(auth_header, source_id)
    else:
        LOG.info(f"Source ID not found for ID: {source_id}")
        return

    try:
        if source_type == Provider.PROVIDER_OCP:
            source_details = sources_network.get_source_details()
            if source_details.get("source_ref"):
                authentication = {"resource_name": source_details.get("source_ref")}
            else:
                raise SourcesHTTPClientError("Unable to find Cluster ID")
        elif source_type in (Provider.PROVIDER_AWS, Provider.PROVIDER_AWS_LOCAL):
            authentication = {"resource_name": sources_network.get_aws_role_arn()}
        elif source_type in (Provider.PROVIDER_AZURE, Provider.PROVIDER_AZURE_LOCAL):
            authentication = {"credentials": sources_network.get_azure_credentials()}
        else:
            LOG.error(f"Unexpected source type: {source_type}")
            return
        storage.add_provider_sources_auth_info(source_id, authentication)
        storage.clear_update_flag(source_id)
        LOG.info(f"Authentication attached to Source ID: {source_id}")
    except SourcesHTTPClientError as error:
        LOG.info(f"Authentication info not available for Source ID: {source_id}")
        sources_network.set_source_status(str(error))


def sources_network_info(source_id, auth_header):
    """
    Get additional sources context from Sources REST API.

    Additional details retrieved from the network includes:
        - Source Name
        - Source ID Type -> AWS, Azure, or OCP
        - Authentication: OCP -> Source uid; AWS -> Network call to Sources Authentication Store

    Details are stored in the Sources database table.

    Args:
        source_id (Integer): Source identifier
        auth_header (String): Authentication Header.

    Returns:
        None

    """
    sources_network = SourcesHTTPClient(auth_header, source_id)
    source_details = sources_network.get_source_details()
    source_name = source_details.get("name")
    source_type_id = int(source_details.get("source_type_id"))
    source_uuid = source_details.get("uid")
    source_type_name = sources_network.get_source_type_name(source_type_id)
    endpoint_id = sources_network.get_endpoint_id()

    if not endpoint_id and source_type_name != SOURCES_OCP_SOURCE_NAME:
        LOG.warning(f"Unable to find endpoint for Source ID: {source_id}")
        return

    source_type = SOURCE_PROVIDER_MAP.get(source_type_name)
    if not source_type:
        LOG.warning(f"Unexpected source type ID: {source_type_id}")
        return

    storage.add_provider_sources_network_info(source_id, source_uuid, source_name, source_type, endpoint_id)
    save_auth_info(auth_header, source_id)


def get_consumer():
    """Create a Kafka consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": Config.SOURCES_KAFKA_ADDRESS,
            "group.id": "hccm-sources",
            "queued.max.messages.kbytes": 1024,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([Config.SOURCES_TOPIC])
    return consumer


def listen_for_messages_loop(application_source_id):  # pragma: no cover
    """Wrap listen_for_messages in while true."""
    consumer = get_consumer()
    LOG.info("Listener started.  Waiting for messages...")
    while True:
        msg_list = consumer.consume()
        if len(msg_list) == 1:
            msg = msg_list.pop()
        else:
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
        try:
            msg = create_msg_processor(kaf_msg, application_source_id)
        except SourcesMessageError:
            return
        if msg and msg.source_id:
            LOG.info(f"Processing message offset: {msg.offset} partition: {msg.partition}")
            topic_partition = TopicPartition(topic=Config.SOURCES_TOPIC, partition=msg.partition, offset=msg.offset)
            LOG.info(f"Cost Management Message to process: {msg}")
            try:
                with transaction.atomic():
                    msg.process()
                    consumer.commit()
            except (InterfaceError, OperationalError) as err:
                close_and_set_db_connection()
                LOG.error(f"{type(err).__name__}: {err}")
                rewind_consumer_to_retry(consumer, topic_partition)
            except (IntegrityError, SourcesHTTPClientError) as err:
                LOG.error(f"{type(err).__name__}: {err}")
                rewind_consumer_to_retry(consumer, topic_partition)
            except SourceNotFoundError:
                LOG.warning(f"Source not found in platform sources. Skipping msg: {msg}")
                consumer.commit()

    except KafkaError as error:
        LOG.error(f"[listen_for_messages] Kafka error encountered: {type(error).__name__}: {error}", exc_info=True)
    except Exception as error:
        LOG.error(f"[listen_for_messages] UNKNOWN error encountered: {type(error).__name__}: {error}", exc_info=True)


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
    sources_obj = msg.get("provider")
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
        f'for Source ID: {str(msg.get("provider").source_id)}'
    )
    try:
        execute_koku_provider_op(msg)
        LOG.info(
            f'Koku provider operation to execute: {msg.get("operation")} '
            f'for Source ID: {str(msg.get("provider").source_id)} complete.'
        )
        if msg.get("operation") != "destroy":
            storage.clear_update_flag(msg.get("provider").source_id)

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
        provider = msg.get("provider")
        source_id = provider.source_id if provider else "unknown"
        LOG.error(
            f"[synchronize_sources] Unexpected synchronization error for Source ID {source_id} "
            f"encountered: {type(error).__name__}: {error}",
            exc_info=True,
        )


def backoff(interval, maximum=120):
    """Exponential back-off."""
    wait = min(maximum, (2 ** interval)) + random.random()
    LOG.info("Sleeping for %.2f seconds.", wait)
    time.sleep(wait)


def is_kafka_connected():  # pragma: no cover
    """
    Check connectability to Kafka messenger.

    This method will block sources integration initialization until
    Kafka is connected.
    """
    count = 0
    result = False
    while not result:
        result = check_kafka_connection()
        if result:
            LOG.info("Test connection to Kafka was successful.")
        else:
            LOG.error("Unable to connect to Kafka server.")
            KAFKA_CONNECTION_ERRORS_COUNTER.inc()
            backoff(count)
            count += 1
    return result


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

    if is_kafka_connected():  # Next, check that Kafka is running
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
