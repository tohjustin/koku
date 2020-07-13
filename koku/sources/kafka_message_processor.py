#
# Copyright 2020 Red Hat, Inc.
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
import json
import logging

from rest_framework.exceptions import ValidationError

from api.provider.models import Provider
from sources import storage
from sources.config import Config
from sources.sources_http_client import SourcesHTTPClient
from sources.sources_http_client import SourcesHTTPClientError


LOG = logging.getLogger(__name__)

KAFKA_APPLICATION_CREATE = "Application.create"
KAFKA_APPLICATION_DESTROY = "Application.destroy"
KAFKA_AUTHENTICATION_CREATE = "Authentication.create"
KAFKA_AUTHENTICATION_UPDATE = "Authentication.update"
KAFKA_SOURCE_UPDATE = "Source.update"
KAFKA_SOURCE_DESTROY = "Source.destroy"
KAFKA_HDR_RH_IDENTITY = "x-rh-identity"
KAFKA_HDR_EVENT_TYPE = "event_type"

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


class SourcesMessageError(ValidationError):
    """Sources Message error."""


class SourceDetails:
    def __init__(self, auth_header, source_id):
        sources_network = SourcesHTTPClient(auth_header, source_id)
        details = sources_network.get_source_details()
        self.source_name = details.get("name")
        self.source_type_id = int(details.get("source_type_id"))
        self.source_uuid = details.get("uid")
        self.source_type_name = sources_network.get_source_type_name(self.source_type_id)
        self.endpoint_id = sources_network.get_endpoint_id()


class KafkaMessageProcessor:
    def __init__(self, msg, event_type, app_type_id):
        try:
            self.value = json.loads(msg.value().decode("utf-8"))
            LOG.debug(f"msg value: {str(self.value)}")
        except (AttributeError, ValueError, TypeError) as error:
            LOG.error("Unable to load message: %s. Error: %s", str(msg.value), str(error))
            raise SourcesMessageError("Unable to load message")
        self.event_type = event_type
        self.offset = msg.offset()
        self.partition = msg.partition()
        self.auth_header = _extract_from_header(msg.headers(), KAFKA_HDR_RH_IDENTITY)
        self.source_id = None

    def __repr__(self):
        return (
            f"Event type: {self.event_type} | Source ID: {self.source_id} |"
            f" Partition: {self.partition} | Offset: {self.offset}"
        )


class ApplicationMsgProcessor(KafkaMessageProcessor):
    def __init__(self, msg, event_type, app_type_id):
        super().__init__(msg, event_type, app_type_id)
        if int(self.value.get("application_type_id")) == app_type_id:
            LOG.debug("Application Message: %s", str(msg))
            self.source_id = int(self.value.get("source_id"))

    def process(self):
        if self.event_type in (KAFKA_APPLICATION_CREATE,):
            storage.create_source_event(self.source_id, self.auth_header, self.offset)

            if storage.is_known_source(self.source_id):
                sources_network_info(self.source_id, self.auth_header)

        elif self.event_type in (KAFKA_APPLICATION_DESTROY,):
            storage.enqueue_source_delete(self.source_id, self.offset, allow_out_of_order=True)


class AuthenticationMsgProcessor(KafkaMessageProcessor):
    def __init__(self, msg, event_type, app_type_id):
        super().__init__(msg, event_type, app_type_id)
        if self.value.get("resource_type") == "Endpoint":
            self.resource_id = int(self.value.get("resource_id"))
            sources_network = SourcesHTTPClient(self.auth_header)
            self.source_id = sources_network.get_source_id_from_endpoint_id(self.resource_id)

    def process(self):
        if self.event_type in (KAFKA_AUTHENTICATION_CREATE,):
            # create source if it does not exist
            storage.create_source_event(self.source_id, self.auth_header, self.offset)

        save_auth_info(self.auth_header, self.source_id)

        if self.event_type in (KAFKA_AUTHENTICATION_UPDATE):
            storage.enqueue_source_update(self.source_id)


class SourceMsgProcessor(KafkaMessageProcessor):
    def __init__(self, msg, event_type, app_type_id):
        super().__init__(msg, event_type, app_type_id)
        LOG.debug("Source Message: %s", str(msg))
        self.source_id = int(self.value.get("id"))

    def process(self):
        if self.event_type in (KAFKA_SOURCE_UPDATE,):
            if storage.is_known_source(self.source_id) is False:
                LOG.info("Update event for unknown source id, skipping...")
                return
            sources_network_info(self.source_id, self.auth_header)
            storage.enqueue_source_update(self.source_id)

        elif self.event_type in (KAFKA_SOURCE_DESTROY,):
            storage.enqueue_source_delete(self.source_id, self.offset)


def _extract_from_header(headers, header_type):
    """Retrieve information from Kafka Headers."""
    for header in headers:
        if header_type in header:
            for item in header:
                if item == header_type:
                    continue
                else:
                    return item.decode("ascii")
    return None


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
    details_obj = SourceDetails(auth_header, source_id)
    if not details_obj.endpoint_id and details_obj.source_type_name != SOURCES_OCP_SOURCE_NAME:
        LOG.warning(f"Unable to find endpoint for Source ID: {source_id}")
        return

    source_type = SOURCE_PROVIDER_MAP.get(details_obj.source_type_name)
    if not source_type:
        LOG.warning(f"Unexpected source type ID: {details_obj.source_type_id}")
        return

    storage.add_provider_sources_network_info(details_obj, source_id)
    save_auth_info(auth_header, source_id)


def create_msg_processor(msg, app_type_id):
    if msg.topic() == Config.SOURCES_TOPIC:
        event_type = _extract_from_header(msg.headers(), KAFKA_HDR_EVENT_TYPE)
        LOG.debug(f"event_type: {str(event_type)}")
        if event_type in (KAFKA_APPLICATION_CREATE, KAFKA_APPLICATION_DESTROY):
            return ApplicationMsgProcessor(msg, event_type, app_type_id)
        elif event_type in (KAFKA_AUTHENTICATION_CREATE, KAFKA_AUTHENTICATION_UPDATE):
            return AuthenticationMsgProcessor(msg, event_type, app_type_id)
        elif event_type in (KAFKA_SOURCE_DESTROY, KAFKA_SOURCE_UPDATE):
            return SourceMsgProcessor(msg, event_type, app_type_id)
        else:
            LOG.debug("Other Message: %s", str(msg))
