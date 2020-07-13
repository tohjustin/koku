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

from sources import storage
from sources.config import Config
from sources.kafka_listener import save_auth_info
from sources.kafka_listener import sources_network_info
from sources.sources_http_client import SourcesHTTPClient


LOG = logging.getLogger(__name__)
KAFKA_APPLICATION_CREATE = "Application.create"
KAFKA_APPLICATION_DESTROY = "Application.destroy"
KAFKA_AUTHENTICATION_CREATE = "Authentication.create"
KAFKA_AUTHENTICATION_UPDATE = "Authentication.update"
KAFKA_SOURCE_UPDATE = "Source.update"
KAFKA_SOURCE_DESTROY = "Source.destroy"
KAFKA_HDR_RH_IDENTITY = "x-rh-identity"
KAFKA_HDR_EVENT_TYPE = "event_type"


class SourcesMessageError(ValidationError):
    """Sources Message error."""


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
