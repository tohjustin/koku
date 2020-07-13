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
"""Sources-Provider Coordinator."""
import logging

from rest_framework.exceptions import ValidationError

from api.provider.provider_builder import ProviderBuilder
from api.provider.provider_builder import ProviderBuilderError
from sources.storage import add_provider_koku_uuid
from sources.storage import clear_update_flag
from sources.storage import destroy_source_event

LOG = logging.getLogger(__name__)


class SourcesProviderCoordinatorError(ValidationError):
    """SourcesProviderCoordinator Error."""

    pass


class SourcesProviderCoordinator:
    """Coordinator to control source and provider operations."""

    def __init__(self, sources_obj):
        """Initialize the client."""
        self.sources_obj = sources_obj
        header = {"x-rh-identity": sources_obj.auth_header, "sources-client": "True"}
        self._source_id = sources_obj.source_id
        self._identity_header = header
        self._provider_builder = ProviderBuilder(self._identity_header)


class SourcesProviderCreator(SourcesProviderCoordinator):
    def perform_operation(self):
        """Call to create provider."""
        LOG.info(f"Creating Koku Provider for Source ID: {self._source_id}")
        try:
            provider = self._provider_builder.create_provider(self.sources_obj)
            add_provider_koku_uuid(self._source_id, provider.uuid)
        except ProviderBuilderError as provider_err:
            raise SourcesProviderCoordinatorError(str(provider_err))
        LOG.info(f"Creating provider {provider.uuid} for Source ID: {self._source_id}")
        return provider


class SourcesProviderUpdater(SourcesProviderCoordinator):
    def perform_operation(self):
        """Call to update provider."""
        try:
            provider = self._provider_builder.update_provider(self.sources_obj)
            clear_update_flag(self._source_id)
        except ProviderBuilderError as provider_err:
            raise SourcesProviderCoordinatorError(str(provider_err))
        LOG.info(f"Updating provider {provider.uuid} for Source ID: {self._source_id}")
        return provider


class SourcesProviderDestroyer(SourcesProviderCoordinator):
    def perform_operation(self):
        """Call to destroy provider."""
        try:
            self._provider_builder.destroy_provider(self.sources_obj.koku_uuid)
            destroy_source_event(self._source_id)
        except ProviderBuilderError as provider_err:
            LOG.error(f"Failed to remove provider. Error: {str(provider_err)}")
        LOG.info(f"Destroying provider {self.sources_obj.koku_uuid} for Source ID: {self._source_id}")


def select_coordinator(sources_obj, operation):
    if operation == "create":
        return SourcesProviderCreator(sources_obj)
    elif operation == "update":
        return SourcesProviderUpdater(sources_obj)
    elif operation == "destroy":
        return SourcesProviderDestroyer(sources_obj)
    else:
        LOG.error(f"[select_coordinator] Unknown operation {operation}")
