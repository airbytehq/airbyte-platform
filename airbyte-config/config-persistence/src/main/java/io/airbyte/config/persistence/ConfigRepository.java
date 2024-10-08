/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Geography;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WorkspaceServiceAccount;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Repository of all SQL queries for the Configs Db. We are moving to persistences scoped by
 * resource.
 */
@Deprecated
@SuppressWarnings("PMD.PreserveStackTrace")
public class ConfigRepository {

  /**
   * Query object for querying connections for a workspace.
   *
   * @param workspaceId workspace to fetch connections for
   * @param sourceId fetch connections with this source id
   * @param destinationId fetch connections with this destination id
   * @param includeDeleted include tombstoned connections
   */
  public record StandardSyncQuery(@Nonnull UUID workspaceId, List<UUID> sourceId, List<UUID> destinationId, boolean includeDeleted) {

  }

  /**
   * Query object for paginated querying of connections in multiple workspaces.
   *
   * @param workspaceIds workspaces to fetch connections for
   * @param sourceId fetch connections with this source id
   * @param destinationId fetch connections with this destination id
   * @param includeDeleted include tombstoned connections
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record StandardSyncsQueryPaginated(
                                            @Nonnull List<UUID> workspaceIds,
                                            List<UUID> sourceId,
                                            List<UUID> destinationId,
                                            boolean includeDeleted,
                                            int pageSize,
                                            int rowOffset) {

  }

  /**
   * Query object for paginated querying of sources/destinations in multiple workspaces.
   *
   * @param workspaceIds workspaces to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   * @param nameContains string to search name contains by
   */
  public record ResourcesQueryPaginated(
                                        @Nonnull List<UUID> workspaceIds,
                                        boolean includeDeleted,
                                        int pageSize,
                                        int rowOffset,
                                        String nameContains) {

  }

  /**
   * Query object for paginated querying of resource in an organization.
   *
   * @param organizationId organization to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record ResourcesByOrganizationQueryPaginated(
                                                      @Nonnull UUID organizationId,
                                                      boolean includeDeleted,
                                                      int pageSize,
                                                      int rowOffset) {

  }

  /**
   * Query object for paginated querying of resource for a user.
   *
   * @param userId user to fetch resources for
   * @param includeDeleted include tombstoned resources
   * @param pageSize limit
   * @param rowOffset offset
   */
  public record ResourcesByUserQueryPaginated(
                                              @Nonnull UUID userId,
                                              boolean includeDeleted,
                                              int pageSize,
                                              int rowOffset) {}

  private final ConnectorBuilderService connectorBuilderService;
  private final OperationService operationService;
  private final WorkspaceService workspaceService;

  @SuppressWarnings("ParameterName")
  @VisibleForTesting
  public ConfigRepository(final ConnectorBuilderService connectorBuilderService,
                          final OperationService operationService,
                          final WorkspaceService workspaceService) {
    this.connectorBuilderService = connectorBuilderService;
    this.operationService = operationService;
    this.workspaceService = workspaceService;
  }

  /**
   * Get workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include tombstoned workspace
   * @return workspace
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Deprecated
  public StandardWorkspace getStandardWorkspaceNoSecrets(final UUID workspaceId, final boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return workspaceService.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, workspaceId.toString());
    }
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombstoned workspace
   * @return workspace, if present.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public Optional<StandardWorkspace> getWorkspaceBySlugOptional(final String slug, final boolean includeTombstone)
      throws IOException {
    return workspaceService.getWorkspaceBySlugOptional(slug, includeTombstone);
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombstoned workspace
   * @return workspace
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Deprecated
  @SuppressWarnings("PMD")
  public StandardWorkspace getWorkspaceBySlug(final String slug, final boolean includeTombstone) throws IOException, ConfigNotFoundException {
    try {
      return workspaceService.getWorkspaceBySlug(slug, includeTombstone);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, slug);
    }
  }

  /**
   * List workspaces.
   *
   * @param includeTombstone include tombstoned workspaces
   * @return workspaces
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardWorkspace> listStandardWorkspaces(final boolean includeTombstone) throws IOException {
    return workspaceService.listStandardWorkspaces(includeTombstone);
  }

  /**
   * List workspaces with given ids.
   *
   * @param includeTombstone include tombstoned workspaces
   * @return workspaces
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardWorkspace> listStandardWorkspacesWithIds(final List<UUID> workspaceIds, final boolean includeTombstone) throws IOException {
    return workspaceService.listStandardWorkspacesWithIds(workspaceIds, includeTombstone);
  }

  /**
   * List ALL workspaces (paginated) with some filtering.
   *
   * @param resourcesQueryPaginated - contains all the information we need to paginate
   * @return A List of StandardWorkspace objects
   * @throws IOException you never know when you IO
   */
  @Deprecated
  public List<StandardWorkspace> listAllWorkspacesPaginated(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return workspaceService.listAllWorkspacesPaginated(
        new io.airbyte.data.services.shared.ResourcesQueryPaginated(
            resourcesQueryPaginated.workspaceIds(),
            resourcesQueryPaginated.includeDeleted(),
            resourcesQueryPaginated.pageSize(),
            resourcesQueryPaginated.rowOffset(),
            resourcesQueryPaginated.nameContains()));
  }

  /**
   * List workspaces (paginated).
   *
   * @param resourcesQueryPaginated - contains all the information we need to paginate
   * @return A List of StandardWorkspace objects
   * @throws IOException you never know when you IO
   */
  @Deprecated
  public List<StandardWorkspace> listStandardWorkspacesPaginated(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return workspaceService.listStandardWorkspacesPaginated(
        new io.airbyte.data.services.shared.ResourcesQueryPaginated(
            resourcesQueryPaginated.workspaceIds(),
            resourcesQueryPaginated.includeDeleted(),
            resourcesQueryPaginated.pageSize(),
            resourcesQueryPaginated.rowOffset(),
            resourcesQueryPaginated.nameContains()));
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from the config-secrets module.
   * <p>
   * Write a StandardWorkspace to the database.
   *
   * @param workspace - The configuration of the workspace
   * @throws JsonValidationException - throws is the workspace is invalid
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeStandardWorkspaceNoSecrets(final StandardWorkspace workspace) throws JsonValidationException, IOException {
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
  }

  /**
   * Set user feedback on workspace.
   *
   * @param workspaceId workspace id.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void setFeedback(final UUID workspaceId) throws IOException {
    workspaceService.setFeedback(workspaceId);
  }

  /**
   * Get workspace for a connection.
   *
   * @param connectionId connection id
   * @param isTombstone include tombstoned workspaces
   * @return workspace to which the connection belongs
   */
  @Deprecated
  @SuppressWarnings("PMD")
  public StandardWorkspace getStandardWorkspaceFromConnection(final UUID connectionId, final boolean isTombstone) throws ConfigNotFoundException {
    try {
      return workspaceService.getStandardWorkspaceFromConnection(connectionId, isTombstone);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Test if workspace id has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param workspaceId id of the workspace
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public boolean workspaceCanUseDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    return workspaceService.workspaceCanUseDefinition(actorDefinitionId, workspaceId);
  }

  /**
   * Test if workspace is has access to a custom connector definition.
   *
   * @param actorDefinitionId custom actor definition id
   * @param workspaceId workspace id
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public boolean workspaceCanUseCustomDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    return workspaceService.workspaceCanUseCustomDefinition(actorDefinitionId, workspaceId);
  }

  /**
   * List active workspace IDs with most recently running jobs within a given time window (in hours).
   *
   * @param timeWindowInHours - integer, e.g. 24, 48, etc
   * @return list of workspace IDs
   * @throws IOException - failed to query data
   */
  @Deprecated
  public List<UUID> listActiveWorkspacesByMostRecentlyRunningJobs(final int timeWindowInHours) throws IOException {
    return workspaceService.listActiveWorkspacesByMostRecentlyRunningJobs(timeWindowInHours);
  }

  /**
   * List connection IDs for active syncs based on the given query.
   *
   * @param standardSyncQuery query
   * @return list of connection IDs
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<UUID> listWorkspaceActiveSyncIds(final StandardSyncQuery standardSyncQuery) throws IOException {
    return workspaceService.listWorkspaceActiveSyncIds(new io.airbyte.data.services.shared.StandardSyncQuery(
        standardSyncQuery.workspaceId(),
        standardSyncQuery.sourceId(),
        standardSyncQuery.destinationId(),
        standardSyncQuery.includeDeleted()));
  }

  /**
   * Get sync operation.
   *
   * @param operationId operation id
   * @return sync operation
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public StandardSyncOperation getStandardSyncOperation(final UUID operationId) throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return operationService.getStandardSyncOperation(operationId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Write standard sync operation.
   *
   * @param standardSyncOperation standard sync operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void writeStandardSyncOperation(final StandardSyncOperation standardSyncOperation) throws IOException {
    operationService.writeStandardSyncOperation(standardSyncOperation);
  }

  /**
   * List standard sync operations.
   *
   * @return standard sync operations.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSyncOperation> listStandardSyncOperations() throws IOException {
    return operationService.listStandardSyncOperations();
  }

  /**
   * Updates {@link io.airbyte.db.instance.configs.jooq.generated.tables.ConnectionOperation} records
   * for the given {@code connectionId}.
   *
   * @param connectionId ID of the associated connection to update operations for
   * @param newOperationIds Set of all operationIds that should be associated to the connection
   * @throws IOException - exception while interacting with the db
   */
  @Deprecated
  public void updateConnectionOperationIds(final UUID connectionId, final Set<UUID> newOperationIds) throws IOException {
    operationService.updateConnectionOperationIds(connectionId, newOperationIds);
  }

  /**
   * Delete standard sync operation.
   *
   * @param standardSyncOperationId standard sync operation id
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void deleteStandardSyncOperation(final UUID standardSyncOperationId) throws IOException {
    operationService.deleteStandardSyncOperation(standardSyncOperationId);
  }

  /**
   * Pair of source and its associated definition.
   * <p>
   * Data-carrier records to hold combined result of query for a Source or Destination and its
   * corresponding Definition. This enables the API layer to process combined information about a
   * Source/Destination/Definition pair without requiring two separate queries and in-memory join
   * operation, because the config models are grouped immediately in the repository layer.
   *
   * @param source source
   * @param definition its corresponding definition
   */
  @VisibleForTesting
  public record SourceAndDefinition(SourceConnection source, StandardSourceDefinition definition) {

  }

  /**
   * Pair of destination and its associated definition.
   *
   * @param destination destination
   * @param definition its corresponding definition
   */
  @VisibleForTesting
  public record DestinationAndDefinition(DestinationConnection destination, StandardDestinationDefinition definition) {

  }

  /**
   * Count connections in workspace.
   *
   * @param workspaceId workspace id
   * @return number of connections in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public int countConnectionsForWorkspace(final UUID workspaceId) throws IOException {
    return workspaceService.countConnectionsForWorkspace(workspaceId);
  }

  /**
   * Count sources in workspace.
   *
   * @param workspaceId workspace id
   * @return number of sources in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public int countSourcesForWorkspace(final UUID workspaceId) throws IOException {
    return workspaceService.countSourcesForWorkspace(workspaceId);
  }

  /**
   * Count destinations in workspace.
   *
   * @param workspaceId workspace id
   * @return number of destinations in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public int countDestinationsForWorkspace(final UUID workspaceId) throws IOException {
    return workspaceService.countDestinationsForWorkspace(workspaceId);
  }

  /**
   * Get workspace service account without secrets.
   *
   * @param workspaceId workspace id
   * @return workspace service account
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  @SuppressWarnings("PMD")
  public WorkspaceServiceAccount getWorkspaceServiceAccountNoSecrets(final UUID workspaceId) throws IOException, ConfigNotFoundException {
    // breaking the pattern of doing a list query, because we never want to list this resource without
    // scoping by workspace id.
    try {
      return workspaceService.getWorkspaceServiceAccountNoSecrets(workspaceId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(ConfigSchema.WORKSPACE_SERVICE_ACCOUNT, workspaceId.toString());
    }
  }

  /**
   * Write workspace service account with no secrets.
   *
   * @param workspaceServiceAccount workspace service account
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void writeWorkspaceServiceAccountNoSecrets(final WorkspaceServiceAccount workspaceServiceAccount) throws IOException {
    workspaceService.writeWorkspaceServiceAccountNoSecrets(workspaceServiceAccount);
  }

  /**
   * Get geography for a workspace.
   *
   * @param workspaceId workspace id
   * @return geography
   * @throws IOException exception while interacting with the db
   */
  @Deprecated
  public Geography getGeographyForWorkspace(final UUID workspaceId) throws IOException {
    return workspaceService.getGeographyForWorkspace(workspaceId);
  }

  /**
   * Specialized query for efficiently determining eligibility for the Free Connector Program. If a
   * workspace has at least one Alpha or Beta connector, users of that workspace will be prompted to
   * sign up for the program. This check is performed on nearly every page load so the query needs to
   * be as efficient as possible.
   * <p>
   * This should only be used for efficiently determining eligibility for the Free Connector Program.
   * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
   * the ReleaseStages.
   *
   * @param workspaceId ID of the workspace to check connectors for
   * @return boolean indicating if an alpha or beta connector exists within the workspace
   */
  @Deprecated
  public boolean getWorkspaceHasAlphaOrBetaConnector(final UUID workspaceId) throws IOException {
    return workspaceService.getWorkspaceHasAlphaOrBetaConnector(workspaceId);
  }

  /**
   * Load all config injection for an actor definition.
   *
   * @param actorDefinitionId id of the actor definition to fetch
   * @return stream of config injection objects
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public Stream<ActorDefinitionConfigInjection> getActorDefinitionConfigInjections(final UUID actorDefinitionId) throws IOException {
    return connectorBuilderService.getActorDefinitionConfigInjections(actorDefinitionId);
  }

  /**
   * Update or create a config injection object. If there is an existing config injection for the
   * given actor definition and path, it is updated. If there isn't yet, a new config injection is
   * created.
   *
   * @param actorDefinitionConfigInjection the config injection object to write to the database
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void writeActorDefinitionConfigInjectionForPath(final ActorDefinitionConfigInjection actorDefinitionConfigInjection) throws IOException {
    connectorBuilderService.writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection);
  }

}
