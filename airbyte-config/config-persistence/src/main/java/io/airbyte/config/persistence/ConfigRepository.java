/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorCatalogFetchEvent;
import io.airbyte.config.ActorCatalogWithUpdatedAt;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.Geography;
import io.airbyte.config.Organization;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WorkspaceServiceAccount;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.HealthCheckService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.OperationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

  private final ActorDefinitionService actorDefinitionService;
  private final CatalogService catalogService;
  private final ConnectionService connectionService;
  private final ConnectorBuilderService connectorBuilderService;
  private final DestinationService destinationService;
  private final HealthCheckService healthCheckService;
  private final OAuthService oAuthService;
  private final OperationService operationService;
  private final OrganizationService organizationService;
  private final SourceService sourceService;
  private final WorkspaceService workspaceService;

  @SuppressWarnings("ParameterName")
  @VisibleForTesting
  public ConfigRepository(final ActorDefinitionService actorDefinitionService,
                          final CatalogService catalogService,
                          final ConnectionService connectionService,
                          final ConnectorBuilderService connectorBuilderService,
                          final DestinationService destinationService,
                          final HealthCheckService healthCheckService,
                          final OAuthService oAuthService,
                          final OperationService operationService,
                          final OrganizationService organizationService,
                          final SourceService sourceService,
                          final WorkspaceService workspaceService) {
    this.actorDefinitionService = actorDefinitionService;
    this.catalogService = catalogService;
    this.connectionService = connectionService;
    this.connectorBuilderService = connectorBuilderService;
    this.destinationService = destinationService;
    this.healthCheckService = healthCheckService;
    this.oAuthService = oAuthService;
    this.operationService = operationService;
    this.organizationService = organizationService;
    this.sourceService = sourceService;
    this.workspaceService = workspaceService;
  }

  /**
   * Conduct a health check by attempting to read from the database. This query needs to be fast as
   * this call can be made multiple times a second.
   *
   * @return true if read succeeds, even if the table is empty, and false if any error happens.
   */
  @Deprecated
  public boolean healthCheck() {
    return healthCheckService.healthCheck();
  }

  /**
   * Get organization.
   *
   * @param organizationId id to use to find the organization
   * @return organization, if present.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public Optional<Organization> getOrganization(final UUID organizationId) throws IOException {
    return organizationService.getOrganization(organizationId);
  }

  /**
   * Write an Organization to the database.
   *
   * @param organization - The configuration of the organization
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeOrganization(final Organization organization) throws IOException {
    organizationService.writeOrganization(organization);
  }

  /**
   * List organizations.
   *
   * @return organizations
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<Organization> listOrganizations() throws IOException {
    return organizationService.listOrganizations();
  }

  /**
   * List organizations (paginated).
   *
   * @param resourcesByOrganizationQueryPaginated - contains all the information we need to paginate
   * @return A List of organizations objects
   * @throws IOException you never know when you IO
   */
  @Deprecated
  public List<Organization> listOrganizationsPaginated(final ResourcesByOrganizationQueryPaginated resourcesByOrganizationQueryPaginated)
      throws IOException {
    final var queryPaginated = new io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated(
        resourcesByOrganizationQueryPaginated.organizationId(),
        resourcesByOrganizationQueryPaginated.includeDeleted(),
        resourcesByOrganizationQueryPaginated.pageSize(),
        resourcesByOrganizationQueryPaginated.rowOffset());
    return organizationService.listOrganizationsPaginated(queryPaginated);
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
   * Get source definition.
   *
   * @param sourceDefinitionId source definition id
   * @return source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Deprecated
  public StandardSourceDefinition getStandardSourceDefinition(final UUID sourceDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return sourceService.getStandardSourceDefinition(sourceDefinitionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get source definition form source.
   *
   * @param sourceId source id
   * @return source definition
   */
  @Deprecated
  public StandardSourceDefinition getSourceDefinitionFromSource(final UUID sourceId) {
    return sourceService.getSourceDefinitionFromSource(sourceId);
  }

  /**
   * Get source definition used by a connection.
   *
   * @param connectionId connection id
   * @return source definition
   */
  @Deprecated
  public StandardSourceDefinition getSourceDefinitionFromConnection(final UUID connectionId) {
    return sourceService.getSourceDefinitionFromConnection(connectionId);
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
   * List standard source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return list source definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardSourceDefinition> listStandardSourceDefinitions(final boolean includeTombstone) throws IOException {
    return sourceService.listStandardSourceDefinitions(includeTombstone);
  }

  /**
   * Get actor definition IDs that are in use.
   *
   * @return list of IDs
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public Set<UUID> getActorDefinitionIdsInUse() throws IOException {
    return actorDefinitionService.getActorDefinitionIdsInUse();
  }

  /**
   * Get actor definition ids to pair of actor type and protocol version.
   *
   * @return map of definition id to pair of actor type and protocol version.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public Map<UUID, Map.Entry<io.airbyte.config.ActorType, Version>> getActorDefinitionToProtocolVersionMap() throws IOException {
    return actorDefinitionService.getActorDefinitionToProtocolVersionMap();
  }

  /**
   * Get a map of all actor definition ids and their default versions.
   *
   * @return map of definition id to default version.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public Map<UUID, ActorDefinitionVersion> getActorDefinitionIdsToDefaultVersionsMap() throws IOException {
    return actorDefinitionService.getActorDefinitionIdsToDefaultVersionsMap();
  }

  /**
   * List public source definitions.
   *
   * @param includeTombstone include tombstoned source
   * @return public source definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardSourceDefinition> listPublicSourceDefinitions(final boolean includeTombstone) throws IOException {
    return sourceService.listPublicSourceDefinitions(includeTombstone);
  }

  /**
   * List granted source definitions for workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned destinations
   * @return list standard source definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardSourceDefinition> listGrantedSourceDefinitions(final UUID workspaceId, final boolean includeTombstones)
      throws IOException {
    return sourceService.listGrantedSourceDefinitions(workspaceId, includeTombstones);
  }

  /**
   * List source to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned definitions
   * @return list of pairs from source definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<Entry<StandardSourceDefinition, Boolean>> listGrantableSourceDefinitions(final UUID workspaceId,
                                                                                       final boolean includeTombstones)
      throws IOException {
    return sourceService.listGrantableSourceDefinitions(workspaceId, includeTombstones);
  }

  /**
   * Update source definition.
   *
   * @param sourceDefinition source definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void updateStandardSourceDefinition(final StandardSourceDefinition sourceDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    try {
      sourceService.updateStandardSourceDefinition(sourceDefinition);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Update the docker image tag for multiple actor definitions at once.
   *
   * @param actorDefinitionIds the list of actor definition ids to update
   * @param targetImageTag the new docker image tag for these actor definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public int updateActorDefinitionsDockerImageTag(final List<UUID> actorDefinitionIds, final String targetImageTag) throws IOException {
    return actorDefinitionService.updateActorDefinitionsDockerImageTag(actorDefinitionIds, targetImageTag);
  }

  /**
   * Get destination definition.
   *
   * @param destinationDefinitionId destination definition id
   * @return destination definition
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Deprecated
  public StandardDestinationDefinition getStandardDestinationDefinition(final UUID destinationDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return destinationService.getStandardDestinationDefinition(destinationDefinitionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get destination definition form destination.
   *
   * @param destinationId destination id
   * @return destination definition
   */
  @Deprecated
  public StandardDestinationDefinition getDestinationDefinitionFromDestination(final UUID destinationId) {
    return destinationService.getDestinationDefinitionFromDestination(destinationId);
  }

  /**
   * Get destination definition used by a connection.
   *
   * @param connectionId connection id
   * @return destination definition
   */
  @Deprecated
  public StandardDestinationDefinition getDestinationDefinitionFromConnection(final UUID connectionId) {
    return destinationService.getDestinationDefinitionFromConnection(connectionId);
  }

  /**
   * List standard destination definitions.
   *
   * @param includeTombstone include tombstoned destinations
   * @return list destination definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardDestinationDefinition> listStandardDestinationDefinitions(final boolean includeTombstone) throws IOException {
    return destinationService.listStandardDestinationDefinitions(includeTombstone);
  }

  /**
   * List public destination definitions.
   *
   * @param includeTombstone include tombstoned destinations
   * @return public destination definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardDestinationDefinition> listPublicDestinationDefinitions(final boolean includeTombstone) throws IOException {
    return destinationService.listPublicDestinationDefinitions(includeTombstone);
  }

  /**
   * List granted destination definitions for workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned destinations
   * @return list standard destination definitions
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<StandardDestinationDefinition> listGrantedDestinationDefinitions(final UUID workspaceId, final boolean includeTombstones)
      throws IOException {
    return destinationService.listGrantedDestinationDefinitions(workspaceId, includeTombstones);
  }

  /**
   * List destinations to which we can give a grant.
   *
   * @param workspaceId workspace id
   * @param includeTombstones include tombstoned definitions
   * @return list of pairs from destination definition and whether it can be granted
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<Entry<StandardDestinationDefinition, Boolean>> listGrantableDestinationDefinitions(final UUID workspaceId,
                                                                                                 final boolean includeTombstones)
      throws IOException {
    return destinationService.listGrantableDestinationDefinitions(workspaceId, includeTombstones);
  }

  /**
   * Update destination definition.
   *
   * @param destinationDefinition destination definition
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void updateStandardDestinationDefinition(final StandardDestinationDefinition destinationDefinition)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    try {
      destinationService.updateStandardDestinationDefinition(destinationDefinition);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Write metadata for a destination connector. Writes global metadata (destination definition) and
   * versioned metadata (info for actor definition version to set as default). Sets the new version as
   * the default version and updates actors accordingly, based on whether the upgrade will be breaking
   * or not.
   *
   * @param destinationDefinition standard destination definition
   * @param actorDefinitionVersion actor definition version
   * @param breakingChangesForDefinition - list of breaking changes for the definition
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion,
                                     final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException {
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, breakingChangesForDefinition);
  }

  /**
   * Write metadata for a destination connector. Writes global metadata (destination definition) and
   * versioned metadata (info for actor definition version to set as default). Sets the new version as
   * the default version and updates actors accordingly, based on whether the upgrade will be breaking
   * or not. Usage of this version of the method assumes no new breaking changes need to be persisted
   * for the definition.
   *
   * @param destinationDefinition standard destination definition
   * @param actorDefinitionVersion actor definition version
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion, List.of());
  }

  /**
   * Write metadata for a source connector. Writes global metadata (source definition, breaking
   * changes) and versioned metadata (info for actor definition version to set as default). Sets the
   * new version as the default version and updates actors accordingly, based on whether the upgrade
   * will be breaking or not.
   *
   * @param sourceDefinition standard source definition
   * @param actorDefinitionVersion actor definition version, containing tag to set as default
   * @param breakingChangesForDefinition - list of breaking changes for the definition
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion,
                                     final List<ActorDefinitionBreakingChange> breakingChangesForDefinition)
      throws IOException {
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, breakingChangesForDefinition);
  }

  /**
   * Write metadata for a source connector. Writes global metadata (source definition) and versioned
   * metadata (info for actor definition version to set as default). Sets the new version as the
   * default version and updates actors accordingly, based on whether the upgrade will be breaking or
   * not. Usage of this version of the method assumes no new breaking changes need to be persisted for
   * the definition.
   *
   * @param sourceDefinition standard source definition
   * @param actorDefinitionVersion actor definition version
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                     final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, List.of());
  }

  /**
   * Write metadata for a custom destination: global metadata (destination definition) and versioned
   * metadata (actor definition version for the version to use).
   *
   * @param destinationDefinition destination definition
   * @param defaultVersion default actor definition version
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeCustomConnectorMetadata(final StandardDestinationDefinition destinationDefinition,
                                           final ActorDefinitionVersion defaultVersion,
                                           final UUID scopeId,
                                           final io.airbyte.config.ScopeType scopeType)
      throws IOException {
    destinationService.writeCustomConnectorMetadata(destinationDefinition, defaultVersion, scopeId, scopeType);
  }

  /**
   * Write metadata for a custom source: global metadata (source definition) and versioned metadata
   * (actor definition version for the version to use).
   *
   * @param sourceDefinition source definition
   * @param defaultVersion default actor definition version
   * @param scopeId scope id
   * @param scopeType enum which defines if the scopeId is a workspace or organization id
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeCustomConnectorMetadata(final StandardSourceDefinition sourceDefinition,
                                           final ActorDefinitionVersion defaultVersion,
                                           final UUID scopeId,
                                           final io.airbyte.config.ScopeType scopeType)
      throws IOException {
    sourceService.writeCustomConnectorMetadata(sourceDefinition, defaultVersion, scopeId, scopeType);
  }

  /**
   * Delete connection.
   *
   * @param syncId connection id
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void deleteStandardSync(final UUID syncId) throws IOException {
    connectionService.deleteStandardSync(syncId);
  }

  /**
   * Write actor definition workspace grant.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId workspace or organization id
   * @param scopeType ScopeType of either workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeActorDefinitionWorkspaceGrant(final UUID actorDefinitionId, final UUID scopeId, final io.airbyte.config.ScopeType scopeType)
      throws IOException {
    actorDefinitionService.writeActorDefinitionWorkspaceGrant(actorDefinitionId, scopeId, scopeType);
  }

  /**
   * Test if grant exists for actor definition and scope.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @return true, if the scope has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public boolean actorDefinitionWorkspaceGrantExists(final UUID actorDefinitionId, final UUID scopeId, final io.airbyte.config.ScopeType scopeType)
      throws IOException {
    return actorDefinitionService.actorDefinitionWorkspaceGrantExists(actorDefinitionId, scopeId, scopeType);
  }

  /**
   * Delete workspace access to actor definition.
   *
   * @param actorDefinitionId actor definition id to remove
   * @param scopeId workspace or organization id
   * @param scopeType enum of workspace or organization
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void deleteActorDefinitionWorkspaceGrant(final UUID actorDefinitionId, final UUID scopeId, final io.airbyte.config.ScopeType scopeType)
      throws IOException {
    actorDefinitionService.deleteActorDefinitionWorkspaceGrant(actorDefinitionId, scopeId, scopeType);
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
   * Test if workspace or organization id has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId id of the workspace or organization
   * @param scopeType enum of workspace or organization
   * @return true, if the workspace or organization has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public boolean scopeCanUseDefinition(final UUID actorDefinitionId, final UUID scopeId, final String scopeType) throws IOException {
    return actorDefinitionService.scopeCanUseDefinition(actorDefinitionId, scopeId, scopeType);
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
   * Returns source with a given id. Does not contain secrets. To hydrate with secrets see the
   * config-secrets module.
   *
   * @param sourceId - id of source to fetch.
   * @return sources
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Deprecated
  public SourceConnection getSourceConnection(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    try {
      return sourceService.getSourceConnection(sourceId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from the config-secrets module.
   * <p>
   * Write a SourceConnection to the database. The configuration of the Source will be a partial
   * configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialSource - The configuration of the Source will be a partial configuration (no
   *        secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeSourceConnectionNoSecrets(final SourceConnection partialSource) throws IOException {
    sourceService.writeSourceConnectionNoSecrets(partialSource);
  }

  /**
   * Delete a source by id.
   *
   * @param sourceId source id
   * @return true if a source was deleted, false otherwise.
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws io.airbyte.data.exceptions.ConfigNotFoundException - throws if no source with that id can
   *         be found.
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public boolean deleteSource(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    try {
      return sourceService.deleteSource(sourceId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Returns all sources in the database. Does not contain secrets. To hydrate with secrets see the
   * config-secrets module.
   *
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<SourceConnection> listSourceConnection() throws IOException {
    return sourceService.listSourceConnection();
  }

  /**
   * Returns all sources for a workspace. Does not contain secrets.
   *
   * @param workspaceId - id of the workspace
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<SourceConnection> listWorkspaceSourceConnection(final UUID workspaceId) throws IOException {
    return sourceService.listWorkspaceSourceConnection(workspaceId);
  }

  /**
   * Returns all sources for a set of workspaces. Does not contain secrets.
   *
   * @param resourcesQueryPaginated - Includes all the things we might want to query
   * @return sources
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<SourceConnection> listWorkspacesSourceConnections(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return sourceService.listWorkspacesSourceConnections(new io.airbyte.data.services.shared.ResourcesQueryPaginated(
        resourcesQueryPaginated.workspaceIds,
        resourcesQueryPaginated.includeDeleted,
        resourcesQueryPaginated.pageSize,
        resourcesQueryPaginated.rowOffset,
        resourcesQueryPaginated.nameContains));
  }

  /**
   * Returns destination with a given id. Does not contain secrets. To hydrate with secrets see the
   * config-secrets module.
   *
   * @param destinationId - id of destination to fetch.
   * @return destinations
   * @throws JsonValidationException - throws if returned destinations are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no destination with that id can be found.
   */
  @Deprecated
  public DestinationConnection getDestinationConnection(final UUID destinationId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return destinationService.getDestinationConnection(destinationId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from the config-secrets module.
   * <p>
   * Write a DestinationConnection to the database. The configuration of the Destination will be a
   * partial configuration (no secrets, just pointer to the secrets store).
   *
   * @param partialDestination - The configuration of the Destination will be a partial configuration
   *        (no secrets, just pointer to the secrets store)
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public void writeDestinationConnectionNoSecrets(final DestinationConnection partialDestination) throws IOException {
    destinationService.writeDestinationConnectionNoSecrets(partialDestination);
  }

  /**
   * Returns all destinations in the database. Does not contain secrets. To hydrate with secrets see
   * the config-secrets module.
   *
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<DestinationConnection> listDestinationConnection() throws IOException {
    return destinationService.listDestinationConnection();
  }

  /**
   * Returns all destinations for a workspace. Does not contain secrets.
   *
   * @param workspaceId - id of the workspace
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<DestinationConnection> listWorkspaceDestinationConnection(final UUID workspaceId) throws IOException {
    return destinationService.listWorkspaceDestinationConnection(workspaceId);
  }

  /**
   * Returns all destinations for a list of workspaces. Does not contain secrets.
   *
   * @param resourcesQueryPaginated - Includes all the things we might want to query
   * @return destinations
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<DestinationConnection> listWorkspacesDestinationConnections(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    final var query = new io.airbyte.data.services.shared.ResourcesQueryPaginated(
        resourcesQueryPaginated.workspaceIds,
        resourcesQueryPaginated.includeDeleted,
        resourcesQueryPaginated.pageSize,
        resourcesQueryPaginated.rowOffset,
        resourcesQueryPaginated.nameContains);
    return destinationService.listWorkspacesDestinationConnections(query);
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
   * Returns all active sources using a definition.
   *
   * @param definitionId - id for the definition
   * @return sources
   * @throws IOException - exception while interacting with the db
   */
  @Deprecated
  public List<SourceConnection> listSourcesForDefinition(final UUID definitionId) throws IOException {
    return sourceService.listSourcesForDefinition(definitionId);
  }

  /**
   * Returns all active sources whose default_version_id is in a given list of version IDs.
   *
   * @param actorDefinitionVersionIds - list of actor definition version ids
   * @return list of SourceConnections
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<SourceConnection> listSourcesWithVersionIds(final List<UUID> actorDefinitionVersionIds) throws IOException {
    return sourceService.listSourcesWithVersionIds(actorDefinitionVersionIds);
  }

  /**
   * Returns all active destinations whose default_version_id is in a given list of version IDs.
   *
   * @param actorDefinitionVersionIds - list of actor definition version ids
   * @return list of DestinationConnections
   * @throws IOException - you never know when you IO
   */
  @Deprecated
  public List<DestinationConnection> listDestinationsWithVersionIds(final List<UUID> actorDefinitionVersionIds) throws IOException {
    return destinationService.listDestinationsWithVersionIds(actorDefinitionVersionIds);
  }

  /**
   * Returns all active destinations using a definition.
   *
   * @param definitionId - id for the definition
   * @return destinations
   * @throws IOException - exception while interacting with the db
   */
  @Deprecated
  public List<DestinationConnection> listDestinationsForDefinition(final UUID definitionId) throws IOException {
    return destinationService.listDestinationsForDefinition(definitionId);
  }

  /**
   * Get connection.
   *
   * @param connectionId connection id
   * @return connection
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public StandardSync getStandardSync(final UUID connectionId) throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      return connectionService.getStandardSync(connectionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Write connection.
   *
   * @param standardSync connection
   * @throws IOException - exception while interacting with the db
   */
  @Deprecated
  public void writeStandardSync(final StandardSync standardSync) throws IOException {
    connectionService.writeStandardSync(standardSync);
  }

  /**
   * List connections.
   *
   * @return connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSync> listStandardSyncs() throws IOException {
    return connectionService.listStandardSyncs();
  }

  /**
   * List connections using operation.
   *
   * @param operationId operation id.
   * @return Connections that use the operation.
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSync> listStandardSyncsUsingOperation(final UUID operationId) throws IOException {
    return connectionService.listStandardSyncsUsingOperation(operationId);
  }

  /**
   * List connections for workspace.
   *
   * @param workspaceId workspace id
   * @param includeDeleted include deleted
   * @return list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSync> listWorkspaceStandardSyncs(final UUID workspaceId, final boolean includeDeleted) throws IOException {
    return connectionService.listWorkspaceStandardSyncs(workspaceId, includeDeleted);
  }

  /**
   * List connections for workspace via a query.
   *
   * @param standardSyncQuery query
   * @return list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSync> listWorkspaceStandardSyncs(final StandardSyncQuery standardSyncQuery) throws IOException {
    final var query = new io.airbyte.data.services.shared.StandardSyncQuery(
        standardSyncQuery.workspaceId(),
        standardSyncQuery.sourceId(),
        standardSyncQuery.destinationId(),
        standardSyncQuery.includeDeleted());
    return connectionService.listWorkspaceStandardSyncs(query);
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
   * List connections. Paginated.
   */
  @Deprecated
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(
                                                                           final List<UUID> workspaceIds,
                                                                           final boolean includeDeleted,
                                                                           final int pageSize,
                                                                           final int rowOffset)
      throws IOException {
    return connectionService.listWorkspaceStandardSyncsPaginated(workspaceIds, includeDeleted, pageSize, rowOffset);
  }

  /**
   * List connections for workspace. Paginated.
   *
   * @param standardSyncsQueryPaginated query
   * @return Map of workspace ID -> list of connections
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public Map<UUID, List<StandardSync>> listWorkspaceStandardSyncsPaginated(final StandardSyncsQueryPaginated standardSyncsQueryPaginated)
      throws IOException {
    final var query = new io.airbyte.data.services.shared.StandardSyncsQueryPaginated(
        standardSyncsQueryPaginated.workspaceIds(),
        standardSyncsQueryPaginated.sourceId(),
        standardSyncsQueryPaginated.destinationId(),
        standardSyncsQueryPaginated.includeDeleted(),
        standardSyncsQueryPaginated.pageSize(),
        standardSyncsQueryPaginated.rowOffset());
    return connectionService.listWorkspaceStandardSyncsPaginated(query);
  }

  /**
   * List connections that use a source.
   *
   * @param sourceId source id
   * @param includeDeleted include deleted
   * @return connections that use the provided source
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StandardSync> listConnectionsBySource(final UUID sourceId, final boolean includeDeleted) throws IOException {
    return connectionService.listConnectionsBySource(sourceId, includeDeleted);
  }

  /**
   * List connections that use a particular actor definition.
   *
   * @param actorDefinitionId id of the source or destination definition.
   * @param actorTypeValue either 'source' or 'destination' enum value.
   * @param includeDeleted whether to include tombstoned records in the return value.
   * @return List of connections that use the actor definition.
   * @throws IOException you never know when you IO
   */
  @Deprecated
  public List<StandardSync> listConnectionsByActorDefinitionIdAndType(final UUID actorDefinitionId,
                                                                      final String actorTypeValue,
                                                                      final boolean includeDeleted)
      throws IOException {
    return connectionService.listConnectionsByActorDefinitionIdAndType(
        actorDefinitionId,
        actorTypeValue,
        includeDeleted);
  }

  /**
   * Disable a list of connections by setting their status to inactive.
   *
   * @param connectionIds list of connection ids to disable
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void disableConnectionsById(final List<UUID> connectionIds) throws IOException {
    connectionService.disableConnectionsById(connectionIds);
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
   * Get source oauth parameter.
   *
   * @param workspaceId workspace id
   * @param sourceDefinitionId source definition id
   * @return source oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    return oAuthService.getSourceOAuthParamByDefinitionIdOptional(workspaceId, sourceDefinitionId);
  }

  /**
   * Write source oauth param.
   *
   * @param sourceOAuthParameter source oauth param
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void writeSourceOAuthParam(final SourceOAuthParameter sourceOAuthParameter) throws IOException {
    oAuthService.writeSourceOAuthParam(sourceOAuthParameter);
  }

  /**
   * Get destination oauth parameter.
   *
   * @param workspaceId workspace id
   * @param destinationDefinitionId destination definition id
   * @return oauth parameters if present
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(final UUID workspaceId,
                                                                                            final UUID destinationDefinitionId)
      throws IOException {
    return oAuthService.getDestinationOAuthParamByDefinitionIdOptional(workspaceId, destinationDefinitionId);
  }

  /**
   * Write destination oauth param.
   *
   * @param destinationOAuthParameter destination oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public void writeDestinationOAuthParam(final DestinationOAuthParameter destinationOAuthParameter) throws IOException {
    oAuthService.writeDestinationOAuthParam(destinationOAuthParameter);
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
   * Get source and definition from sources ids.
   *
   * @param sourceIds source ids
   * @return pair of source and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<SourceAndDefinition> getSourceAndDefinitionsFromSourceIds(final List<UUID> sourceIds) throws IOException {
    return sourceService.getSourceAndDefinitionsFromSourceIds(sourceIds)
        .stream()
        .map(record -> new SourceAndDefinition(record.source(), record.definition()))
        .toList();
  }

  /**
   * Get destination and definition from destinations ids.
   *
   * @param destinationIds destination ids
   * @return pair of destination and definition
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<DestinationAndDefinition> getDestinationAndDefinitionsFromDestinationIds(final List<UUID> destinationIds) throws IOException {
    return destinationService.getDestinationAndDefinitionsFromDestinationIds(destinationIds)
        .stream()
        .map(record -> new DestinationAndDefinition(record.destination(), record.definition()))
        .toList();
  }

  /**
   * Get actor catalog.
   *
   * @param actorCatalogId actor catalog id
   * @return actor catalog
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public ActorCatalog getActorCatalogById(final UUID actorCatalogId)
      throws IOException, ConfigNotFoundException {
    try {
      return catalogService.getActorCatalogById(actorCatalogId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get most actor catalog for source.
   *
   * @param actorId actor id
   * @param actorVersion actor definition version used to make this actor
   * @param configHash config hash for actor
   * @return actor catalog for config has and actor version
   * @throws IOException - error while interacting with db
   */
  @Deprecated
  public Optional<ActorCatalog> getActorCatalog(final UUID actorId,
                                                final String actorVersion,
                                                final String configHash)
      throws IOException {
    return catalogService.getActorCatalog(actorId, actorVersion, configHash);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog with updated at
   * @throws IOException - error while interacting with db
   */
  @Deprecated
  public Optional<ActorCatalogWithUpdatedAt> getMostRecentSourceActorCatalog(final UUID sourceId) throws IOException {
    return catalogService.getMostRecentSourceActorCatalog(sourceId);
  }

  /**
   * Get most recent actor catalog for source.
   *
   * @param sourceId source id
   * @return current actor catalog
   * @throws IOException - error while interacting with db
   */
  @Deprecated
  public Optional<ActorCatalog> getMostRecentActorCatalogForSource(final UUID sourceId) throws IOException {
    return catalogService.getMostRecentActorCatalogForSource(sourceId);
  }

  /**
   * Get most recent actor catalog fetch event for source.
   *
   * @param sourceId source id
   * @return last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  @Deprecated
  public Optional<ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSource(final UUID sourceId) throws IOException {
    return catalogService.getMostRecentActorCatalogFetchEventForSource(sourceId);
  }

  /**
   * Get most recent actor catalog fetch event for sources.
   *
   * @param sourceIds source ids
   * @return map of source id to the last actor catalog fetch event
   * @throws IOException - error while interacting with db
   */
  @SuppressWarnings({"unused", "SqlNoDataSourceInspection"})
  @Deprecated
  public Map<UUID, ActorCatalogFetchEvent> getMostRecentActorCatalogFetchEventForSources(final List<UUID> sourceIds) throws IOException {
    return catalogService.getMostRecentActorCatalogFetchEventForSources(sourceIds);
  }

  /**
   * Stores source catalog information.
   * <p>
   * This function is called each time the schema of a source is fetched. This can occur because the
   * source is set up for the first time, because the configuration or version of the connector
   * changed or because the user explicitly requested a schema refresh. Schemas are stored separately
   * and de-duplicated upon insertion. Once a schema has been successfully stored, a call to
   * getActorCatalog(actorId, connectionVersion, configurationHash) will return the most recent schema
   * stored for those parameters.
   *
   * @param catalog - catalog that was fetched.
   * @param actorId - actor the catalog was fetched by
   * @param connectorVersion - version of the connector when catalog was fetched
   * @param configurationHash - hash of the config of the connector when catalog was fetched
   * @return The identifier (UUID) of the fetch event inserted in the database
   * @throws IOException - error while interacting with db
   */
  @Deprecated
  public UUID writeActorCatalogFetchEvent(final AirbyteCatalog catalog,
                                          final UUID actorId,
                                          final String connectorVersion,
                                          final String configurationHash)
      throws IOException {
    return catalogService.writeActorCatalogFetchEvent(
        catalog,
        actorId,
        connectorVersion,
        configurationHash);
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
   * Get all streams for connection.
   *
   * @param connectionId connection id
   * @return list of streams for connection
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public List<StreamDescriptor> getAllStreamsForConnection(final UUID connectionId) throws ConfigNotFoundException, IOException {
    try {
      return connectionService.getAllStreamsForConnection(connectionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get configured catalog for connection.
   *
   * @param connectionId connection id
   * @return configured catalog
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Deprecated
  public ConfiguredAirbyteCatalog getConfiguredCatalogForConnection(final UUID connectionId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    try {
      return connectionService.getConfiguredCatalogForConnection(connectionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get geography for a connection.
   *
   * @param connectionId connection id
   * @return geography
   * @throws IOException exception while interacting with the db
   */
  @Deprecated
  public Geography getGeographyForConnection(final UUID connectionId) throws IOException {
    return connectionService.getGeographyForConnection(connectionId);
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
   * Specialized query for efficiently determining a connection's eligibility for the Free Connector
   * Program. If a connection has at least one Alpha or Beta connector, it will be free to use as long
   * as the workspace is enrolled in the Free Connector Program. This check is used to allow free
   * connections to continue running even when a workspace runs out of credits.
   * <p>
   * This should only be used for efficiently determining eligibility for the Free Connector Program.
   * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
   * the ReleaseStages.
   *
   * @param connectionId ID of the connection to check connectors for
   * @return boolean indicating if an alpha or beta connector is used by the connection
   */
  @Deprecated
  public boolean getConnectionHasAlphaOrBetaConnector(final UUID connectionId) throws IOException {
    return connectionService.getConnectionHasAlphaOrBetaConnector(connectionId);
  }

  /**
   * Get connector builder project.
   *
   * @param builderProjectId project id
   * @param fetchManifestDraft manifest draft
   * @return builder project
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException if build project is not found
   */
  @Deprecated
  public ConnectorBuilderProject getConnectorBuilderProject(final UUID builderProjectId, final boolean fetchManifestDraft)
      throws IOException, ConfigNotFoundException {
    try {
      return connectorBuilderService.getConnectorBuilderProject(builderProjectId, fetchManifestDraft);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Return a versioned manifest associated with a builder project.
   *
   * @param builderProjectId ID of the connector_builder_project
   * @param version the version of the manifest
   * @return ConnectorBuilderProjectVersionedManifest matching the builderProjectId
   * @throws ConfigNotFoundException ensures that there a connector_builder_project matching the
   *         `builderProjectId`, a declarative_manifest with the specified version associated with the
   *         builder project and an active_declarative_manifest. If either of these conditions is not
   *         true, this error is thrown
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public ConnectorBuilderProjectVersionedManifest getVersionedConnectorBuilderProject(final UUID builderProjectId, final Long version)
      throws ConfigNotFoundException, IOException {
    try {
      return connectorBuilderService.getVersionedConnectorBuilderProject(builderProjectId, version);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Get connector builder project from a workspace id.
   *
   * @param workspaceId workspace id
   * @return builder project
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public Stream<ConnectorBuilderProject> getConnectorBuilderProjectsByWorkspace(@Nonnull final UUID workspaceId) throws IOException {
    return connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId);
  }

  /**
   * Delete builder project.
   *
   * @param builderProjectId builder project to delete
   * @return true if successful
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public boolean deleteBuilderProject(final UUID builderProjectId) throws IOException {
    return connectorBuilderService.deleteBuilderProject(builderProjectId);
  }

  /**
   * Write name and draft of a builder project. If it doesn't exist under the specified id, it is
   * created.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void writeBuilderProjectDraft(final UUID projectId, final UUID workspaceId, final String name, final JsonNode manifestDraft)
      throws IOException {
    connectorBuilderService.writeBuilderProjectDraft(projectId, workspaceId, name, manifestDraft);
  }

  /**
   * Nullify the manifest draft of a builder project.
   *
   * @param projectId the id of the project
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void deleteBuilderProjectDraft(final UUID projectId) throws IOException {
    connectorBuilderService.deleteBuilderProjectDraft(projectId);
  }

  /**
   * Nullify the manifest draft of the builder project associated with the provided actor definition
   * ID and workspace ID.
   *
   * @param actorDefinitionId the id of the actor definition to which the project is linked
   * @param workspaceId the id of the workspace containing the project
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void deleteManifestDraftForActorDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    connectorBuilderService.deleteManifestDraftForActorDefinition(actorDefinitionId, workspaceId);
  }

  /**
   * Write name and draft of a builder project. The actor_definition is also updated to match the new
   * builder project name.
   * <p>
   * Actor definition updated this way should always be private (i.e. public=false). As an additional
   * protection, we want to shield ourselves from users updating public actor definition and
   * therefore, the name of the actor definition won't be updated if the actor definition is not
   * public. See
   * https://github.com/airbytehq/airbyte-platform-internal/pull/5289#discussion_r1142757109.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @param actorDefinitionId the id of the associated actor definition
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void updateBuilderProjectAndActorDefinition(final UUID projectId,
                                                     final UUID workspaceId,
                                                     final String name,
                                                     final JsonNode manifestDraft,
                                                     final UUID actorDefinitionId)
      throws IOException {
    connectorBuilderService.updateBuilderProjectAndActorDefinition(projectId, workspaceId, name, manifestDraft, actorDefinitionId);
  }

  /**
   * Write a builder project to the db.
   *
   * @param builderProjectId builder project to update
   * @param actorDefinitionId the actor definition id associated with the connector builder project
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void assignActorDefinitionToConnectorBuilderProject(final UUID builderProjectId, final UUID actorDefinitionId) throws IOException {
    connectorBuilderService.assignActorDefinitionToConnectorBuilderProject(builderProjectId, actorDefinitionId);
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   * <p>
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   * <p>
   * Since we decided not to validate this using the signature of the method, we will validate that
   * runtime and IllegalArgumentException if there is a mismatch.
   * <p>
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   * <p>
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param declarativeManifest declarative manifest version to create and make active
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being created
   * @throws IOException exception while interacting with db
   * @throws IllegalArgumentException if there is a mismatch between the different arguments
   */
  @Deprecated
  public void createDeclarativeManifestAsActiveVersion(final DeclarativeManifest declarativeManifest,
                                                       final ActorDefinitionConfigInjection configInjection,
                                                       final ConnectorSpecification connectorSpecification)
      throws IOException {
    connectorBuilderService.createDeclarativeManifestAsActiveVersion(declarativeManifest, configInjection, connectorSpecification);
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   * <p>
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   * <p>
   * At that point, we can only hope the user won't cause data consistency issue using this method
   * <p>
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   * <p>
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param sourceDefinitionId actor definition to update
   * @param version the version of the manifest to make active. declarative_manifest.version must
   *        already exist
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being made active
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void setDeclarativeSourceActiveVersion(final UUID sourceDefinitionId,
                                                final Long version,
                                                final ActorDefinitionConfigInjection configInjection,
                                                final ConnectorSpecification connectorSpecification)
      throws IOException {
    connectorBuilderService.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, configInjection, connectorSpecification);
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

  /**
   * Insert a declarative manifest. If DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and
   * DECLARATIVE_MANIFEST.VERSION is already in the DB, an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void insertDeclarativeManifest(final DeclarativeManifest declarativeManifest) throws IOException {
    connectorBuilderService.insertDeclarativeManifest(declarativeManifest);
  }

  /**
   * Insert a declarative manifest and its associated active declarative manifest. If
   * DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and DECLARATIVE_MANIFEST.VERSION is already in the DB,
   * an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public void insertActiveDeclarativeManifest(final DeclarativeManifest declarativeManifest) throws IOException {
    connectorBuilderService.insertActiveDeclarativeManifest(declarativeManifest);
  }

  /**
   * Read all declarative manifests by actor definition id without the manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public Stream<DeclarativeManifest> getDeclarativeManifestsByActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    return connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(actorDefinitionId);
  }

  /**
   * Read declarative manifest by actor definition id and version with manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @param version the version of the declarative manifest
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         and DECLARATIVE_MANIFEST.VERSION
   */
  @Deprecated
  public DeclarativeManifest getDeclarativeManifestByActorDefinitionIdAndVersion(final UUID actorDefinitionId, final long version)
      throws IOException, ConfigNotFoundException {
    try {
      return connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(actorDefinitionId, version);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Read currently active declarative manifest by actor definition id by joining with
   * active_declarative_manifest for the same actor definition id with manifest.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         that matches the version of an active manifest
   */
  @Deprecated
  public DeclarativeManifest getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(final UUID actorDefinitionId)
      throws IOException, ConfigNotFoundException {
    try {
      return connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * Read all actor definition ids which have an active declarative manifest pointing to them.
   *
   * @throws IOException exception while interacting with db
   */
  @Deprecated
  public Stream<UUID> getActorDefinitionIdsWithActiveDeclarativeManifest() throws IOException {
    return connectorBuilderService.getActorDefinitionIdsWithActiveDeclarativeManifest();
  }

  /**
   * Insert an actor definition version.
   *
   * @param actorDefinitionVersion - actor definition version to insert
   * @throws IOException - you never know when you io
   * @returns the POJO associated with the actor definition version inserted. Contains the versionId
   *          field from the DB.
   */
  @Deprecated
  public ActorDefinitionVersion writeActorDefinitionVersion(final ActorDefinitionVersion actorDefinitionVersion) throws IOException {
    return actorDefinitionService.writeActorDefinitionVersion(actorDefinitionVersion);
  }

  /**
   * Get the actor definition version associated with an actor definition and a docker image tag.
   *
   * @param actorDefinitionId - actor definition id
   * @param dockerImageTag - docker image tag
   * @return actor definition version if there is an entry in the DB already for this version,
   *         otherwise an empty optional
   * @throws IOException - you never know when you io
   */
  @Deprecated
  @VisibleForTesting
  public Optional<ActorDefinitionVersion> getActorDefinitionVersion(final UUID actorDefinitionId, final String dockerImageTag)
      throws IOException {
    return actorDefinitionService.getActorDefinitionVersion(actorDefinitionId, dockerImageTag);
  }

  /**
   * Get an actor definition version by ID.
   *
   * @param actorDefinitionVersionId - actor definition version id
   * @return actor definition version
   * @throws ConfigNotFoundException if an actor definition version with the provided ID does not
   *         exist
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public ActorDefinitionVersion getActorDefinitionVersion(final UUID actorDefinitionVersionId) throws IOException, ConfigNotFoundException {
    try {
      return actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersionId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  /**
   * List all actor definition versions for a given actor definition.
   *
   * @param actorDefinitionId - actor definition id
   * @return list of actor definition versions
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public List<ActorDefinitionVersion> listActorDefinitionVersionsForDefinition(final UUID actorDefinitionId) throws IOException {
    return actorDefinitionService.listActorDefinitionVersionsForDefinition(actorDefinitionId);
  }

  /**
   * Get actor definition versions by ID.
   *
   * @param actorDefinitionVersionIds - actor definition version ids
   * @return list of actor definition version
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public List<ActorDefinitionVersion> getActorDefinitionVersions(final List<UUID> actorDefinitionVersionIds) throws IOException {
    return actorDefinitionService.getActorDefinitionVersions(actorDefinitionVersionIds);
  }

  /**
   * Set the default version for an actor.
   *
   * @param actorId - actor id
   * @param actorDefinitionVersionId - actor definition version id
   */
  @Deprecated
  public void setActorDefaultVersion(final UUID actorId, final UUID actorDefinitionVersionId) throws IOException {
    actorDefinitionService.setActorDefaultVersion(actorId, actorDefinitionVersionId);
  }

  /**
   * Get the list of breaking changes available affecting an actor definition.
   *
   * @param actorDefinitionId - actor definition id
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinition(final UUID actorDefinitionId) throws IOException {
    return actorDefinitionService.listBreakingChangesForActorDefinition(actorDefinitionId);
  }

  /**
   * Set the support state for a list of actor definition versions.
   *
   * @param actorDefinitionVersionIds - actor definition version ids to update
   * @param supportState - support state to update to
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public void setActorDefinitionVersionSupportStates(final List<UUID> actorDefinitionVersionIds,
                                                     final ActorDefinitionVersion.SupportState supportState)
      throws IOException {
    actorDefinitionService.setActorDefinitionVersionSupportStates(actorDefinitionVersionIds, supportState);
  }

  /**
   * Get the list of breaking changes available affecting an actor definition version.
   * <p>
   * "Affecting" breaking changes are those between the provided version (non-inclusive) and the actor
   * definition default version (inclusive).
   *
   * @param actorDefinitionVersion - actor definition version
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public List<ActorDefinitionBreakingChange> listBreakingChangesForActorDefinitionVersion(final ActorDefinitionVersion actorDefinitionVersion)
      throws IOException {
    return actorDefinitionService.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion);
  }

  /**
   * List all breaking changes.
   *
   * @return list of breaking changes
   * @throws IOException - you never know when you io
   */
  @Deprecated
  public List<ActorDefinitionBreakingChange> listBreakingChanges() throws IOException {
    return actorDefinitionService.listBreakingChanges();
  }

  @Deprecated
  public Set<Long> listEarlySyncJobs(final int freeUsageInterval, final int jobsFetchRange)
      throws IOException {
    return connectionService.listEarlySyncJobs(freeUsageInterval, jobsFetchRange);
  }

  @Deprecated
  public Optional<SourceOAuthParameter> getSourceOAuthParameterOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    return oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId);
  }

  @Deprecated
  public Optional<DestinationOAuthParameter> getDestinationOAuthParameterOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    return oAuthService.getDestinationOAuthParameterOptional(workspaceId, sourceDefinitionId);
  }

}
