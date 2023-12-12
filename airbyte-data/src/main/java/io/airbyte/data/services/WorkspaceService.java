/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services;

import io.airbyte.config.Geography;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WorkspaceServiceAccount;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * A service that manages workspaces.
 */
public interface WorkspaceService {

  StandardWorkspace getStandardWorkspaceNoSecrets(UUID workspaceId, boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  Optional<StandardWorkspace> getWorkspaceBySlugOptional(String slug, boolean includeTombstone) throws IOException;

  StandardWorkspace getWorkspaceBySlug(String slug, boolean includeTombstone) throws IOException, ConfigNotFoundException;

  List<StandardWorkspace> listStandardWorkspaces(boolean includeTombstone) throws IOException;

  List<StandardWorkspace> listAllWorkspacesPaginated(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  Stream<StandardWorkspace> listWorkspaceQuery(Optional<List<UUID>> workspaceId, boolean includeTombstone) throws IOException;

  List<StandardWorkspace> listStandardWorkspacesPaginated(ResourcesQueryPaginated resourcesQueryPaginated) throws IOException;

  StandardWorkspace getStandardWorkspaceFromConnection(UUID connectionId, boolean isTombstone) throws ConfigNotFoundException;

  void writeStandardWorkspaceNoSecrets(StandardWorkspace workspace) throws JsonValidationException, IOException;

  void setFeedback(UUID workspaceId) throws IOException;

  boolean workspaceCanUseDefinition(UUID actorDefinitionId, UUID workspaceId) throws IOException;

  boolean workspaceCanUseCustomDefinition(UUID actorDefinitionId, UUID workspaceId) throws IOException;

  List<UUID> listActiveWorkspacesByMostRecentlyRunningJobs(int timeWindowInHours) throws IOException;

  int countConnectionsForWorkspace(UUID workspaceId) throws IOException;

  int countSourcesForWorkspace(UUID workspaceId) throws IOException;

  int countDestinationsForWorkspace(UUID workspaceId) throws IOException;

  WorkspaceServiceAccount getWorkspaceServiceAccountNoSecrets(UUID workspaceId) throws IOException, ConfigNotFoundException;

  void writeWorkspaceServiceAccountNoSecrets(WorkspaceServiceAccount workspaceServiceAccount) throws IOException;

  Geography getGeographyForWorkspace(UUID workspaceId) throws IOException;

  boolean getWorkspaceHasAlphaOrBetaConnector(UUID workspaceId) throws IOException;

  List<UUID> listWorkspaceActiveSyncIds(final StandardSyncQuery standardSyncQuery) throws IOException;

  List<StandardWorkspace> listStandardWorkspacesWithIds(final List<UUID> workspaceIds, final boolean includeTombstone) throws IOException;

  Optional<UUID> getOrganizationIdFromWorkspaceId(UUID scopeId) throws IOException;

  StandardWorkspace getWorkspaceWithSecrets(UUID workspaceId, boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException;

  void writeWorkspaceWithSecrets(StandardWorkspace workspace) throws JsonValidationException, IOException, ConfigNotFoundException;

}
