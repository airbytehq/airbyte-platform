/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.Geography;
import io.airbyte.config.StandardSync;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.data.services.shared.StandardSyncsQueryPaginated;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This service is used to manage connections.
 */
public interface ConnectionService {

  void deleteStandardSync(UUID syncId) throws IOException;

  StandardSync getStandardSync(UUID connectionId) throws JsonValidationException, IOException, ConfigNotFoundException;

  void writeStandardSync(StandardSync standardSync) throws IOException;

  List<StandardSync> listStandardSyncs() throws IOException;

  List<StandardSync> listStandardSyncsUsingOperation(UUID operationId) throws IOException;

  List<StandardSync> listWorkspaceStandardSyncs(UUID workspaceId, boolean includeDeleted) throws IOException;

  List<StandardSync> listWorkspaceStandardSyncs(StandardSyncQuery standardSyncQuery) throws IOException;

  Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(List<UUID> workspaceIds, boolean includeDeleted, int pageSize, int rowOffset)
      throws IOException;

  Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(StandardSyncsQueryPaginated standardSyncsQueryPaginated) throws IOException;

  List<StandardSync> listConnectionsBySource(UUID sourceId, boolean includeDeleted) throws IOException;

  List<StandardSync> listConnectionsByActorDefinitionIdAndType(UUID actorDefinitionId, String actorTypeValue, boolean includeDeleted)
      throws IOException;

  List<StreamDescriptor> getAllStreamsForConnection(UUID connectionId) throws ConfigNotFoundException, IOException;

  ConfiguredAirbyteCatalog getConfiguredCatalogForConnection(UUID connectionId) throws JsonValidationException, ConfigNotFoundException, IOException;

  Geography getGeographyForConnection(UUID connectionId) throws IOException;

  boolean getConnectionHasAlphaOrBetaConnector(UUID connectionId) throws IOException;

  Set<Long> listEarlySyncJobs(final int freeUsageInterval, final int jobsFetchRange) throws IOException;

  void disableConnectionsById(final List<UUID> connectionIds) throws IOException;

}
