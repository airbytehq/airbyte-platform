/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.metrics.lib.MetricTags.SOURCE_ID;
import static io.airbyte.metrics.lib.MetricTags.WORKSPACE_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DiscoverCatalogResult;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PartialSourceUpdate;
import io.airbyte.api.model.generated.SourceCreate;
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.SourceSnippetRead;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.entitlements.Entitlement;
import io.airbyte.commons.entitlements.LicenseEntitlementChecker;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * SourceHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings({"ParameterName", "PMD.AvoidDuplicateLiterals"})
@Singleton
public class SourceHandler {

  private final Supplier<UUID> uuidGenerator;
  private final CatalogService catalogService;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final JsonSchemaValidator validator;
  private final ConnectionsHandler connectionsHandler;
  private final ConfigurationUpdate configurationUpdate;
  private final JsonSecretsProcessor secretsProcessor;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;
  private final FeatureFlagClient featureFlagClient;
  private final SourceService sourceService;
  private final WorkspaceService workspaceService;
  private final WorkspaceHelper workspaceHelper;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final LicenseEntitlementChecker licenseEntitlementChecker;
  private final CatalogConverter catalogConverter;
  private final ApiPojoConverters apiPojoConverters;
  private final MetricClient metricClient;
  private final Configs.AirbyteEdition airbyteEdition;

  @VisibleForTesting
  public SourceHandler(final CatalogService catalogService,
                       final SecretsRepositoryReader secretsRepositoryReader,
                       final JsonSchemaValidator integrationSchemaValidation,
                       final ConnectionsHandler connectionsHandler,
                       @Named("uuidGenerator") final Supplier<UUID> uuidGenerator,
                       @Named("jsonSecretsProcessorWithCopy") final JsonSecretsProcessor secretsProcessor,
                       final ConfigurationUpdate configurationUpdate,
                       final OAuthConfigSupplier oAuthConfigSupplier,
                       final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                       final FeatureFlagClient featureFlagClient,
                       final SourceService sourceService,
                       final WorkspaceService workspaceService,
                       final WorkspaceHelper workspaceHelper,
                       final SecretPersistenceConfigService secretPersistenceConfigService,
                       final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                       final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater,
                       final LicenseEntitlementChecker licenseEntitlementChecker,
                       final CatalogConverter catalogConverter,
                       final ApiPojoConverters apiPojoConverters,
                       final MetricClient metricClient,
                       final Configs.AirbyteEdition airbyteEdition) {
    this.catalogService = catalogService;
    this.secretsRepositoryReader = secretsRepositoryReader;
    validator = integrationSchemaValidation;
    this.connectionsHandler = connectionsHandler;
    this.uuidGenerator = uuidGenerator;
    this.configurationUpdate = configurationUpdate;
    this.secretsProcessor = secretsProcessor;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.featureFlagClient = featureFlagClient;
    this.sourceService = sourceService;
    this.workspaceService = workspaceService;
    this.workspaceHelper = workspaceHelper;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater;
    this.licenseEntitlementChecker = licenseEntitlementChecker;
    this.catalogConverter = catalogConverter;
    this.apiPojoConverters = apiPojoConverters;
    this.metricClient = metricClient;
    this.airbyteEdition = airbyteEdition;
  }

  public SourceRead createSourceWithOptionalSecret(final SourceCreate sourceCreate)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.config.persistence.ConfigNotFoundException {
    if (sourceCreate.getSecretId() != null && !sourceCreate.getSecretId().isBlank()) {
      final JsonNode hydratedSecret = hydrateOAuthResponseSecret(sourceCreate.getSecretId(), sourceCreate.getWorkspaceId());
      final ConnectorSpecification spec =
          getSpecFromSourceDefinitionIdForWorkspace(sourceCreate.getSourceDefinitionId(), sourceCreate.getWorkspaceId());
      // add OAuth Response data to connection configuration
      sourceCreate.setConnectionConfiguration(
          OAuthSecretHelper.setSecretsInConnectionConfiguration(spec, hydratedSecret,
              sourceCreate.getConnectionConfiguration()));
    }
    return createSource(sourceCreate);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  @Trace
  public SourceRead updateSourceWithOptionalSecret(final PartialSourceUpdate partialSourceUpdate)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final ConnectorSpecification spec = getSpecFromSourceId(partialSourceUpdate.getSourceId());
    ApmTraceUtils.addTagsToTrace(
        Map.of(
            SOURCE_ID, partialSourceUpdate.getSourceId().toString()));
    if (partialSourceUpdate.getSecretId() != null && !partialSourceUpdate.getSecretId().isBlank()) {
      final SourceConnection sourceConnection;
      try {
        sourceConnection = sourceService.getSourceConnection(partialSourceUpdate.getSourceId());
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
      final JsonNode hydratedSecret = hydrateOAuthResponseSecret(partialSourceUpdate.getSecretId(), sourceConnection.getWorkspaceId());
      // add OAuth Response data to connection configuration
      partialSourceUpdate.setConnectionConfiguration(
          OAuthSecretHelper.setSecretsInConnectionConfiguration(spec, hydratedSecret,
              Optional.ofNullable(partialSourceUpdate.getConnectionConfiguration()).orElse(Jsons.emptyObject())));
      ApmTraceUtils.addTagsToTrace(
          Map.of(
              "oauth_secret", true));
    } else {
      // We aren't using a secret to update the source so no server provided credentials should have been
      // passed in.
      OAuthSecretHelper.validateNoSecretsInConfiguration(spec, partialSourceUpdate.getConnectionConfiguration());
    }
    return partialUpdateSource(partialSourceUpdate);
  }

  @VisibleForTesting
  public SourceRead createSource(final SourceCreate sourceCreate)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    if (sourceCreate.getResourceAllocation() != null && airbyteEdition == Configs.AirbyteEdition.CLOUD) {
      throw new BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition));
    }

    // validate configuration
    final ConnectorSpecification spec = getSpecFromSourceDefinitionIdForWorkspace(
        sourceCreate.getSourceDefinitionId(), sourceCreate.getWorkspaceId());
    validateSource(spec, sourceCreate.getConnectionConfiguration());

    // persist
    final UUID sourceId = uuidGenerator.get();
    persistSourceConnection(
        sourceCreate.getName() != null ? sourceCreate.getName() : "default",
        sourceCreate.getSourceDefinitionId(),
        sourceCreate.getWorkspaceId(),
        sourceId,
        false,
        sourceCreate.getConnectionConfiguration(),
        spec,
        sourceCreate.getResourceAllocation());

    // read configuration from db
    return buildSourceRead(sourceService.getSourceConnection(sourceId), spec);
  }

  public SourceRead partialUpdateSource(final PartialSourceUpdate partialSourceUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    if (partialSourceUpdate.getResourceAllocation() != null && airbyteEdition == Configs.AirbyteEdition.CLOUD) {
      throw new BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition));
    }

    final UUID sourceId = partialSourceUpdate.getSourceId();
    final SourceConnection updatedSource = configurationUpdate
        .partialSource(sourceId, partialSourceUpdate.getName(),
            partialSourceUpdate.getConnectionConfiguration());
    final ConnectorSpecification spec = getSpecFromSourceId(sourceId);
    validateSource(spec, updatedSource.getConfiguration());

    ApmTraceUtils.addTagsToTrace(Map.of(WORKSPACE_ID, updatedSource.getWorkspaceId().toString()));

    // persist
    persistSourceConnection(
        updatedSource.getName(),
        updatedSource.getSourceDefinitionId(),
        updatedSource.getWorkspaceId(),
        updatedSource.getSourceId(),
        updatedSource.getTombstone(),
        updatedSource.getConfiguration(),
        spec,
        partialSourceUpdate.getResourceAllocation());

    // read configuration from db
    return buildSourceRead(sourceService.getSourceConnection(sourceId), spec);
  }

  @Trace
  public SourceRead updateSource(final SourceUpdate sourceUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {
    if (sourceUpdate.getResourceAllocation() != null && airbyteEdition == Configs.AirbyteEdition.CLOUD) {
      throw new BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition));
    }

    final UUID sourceId = sourceUpdate.getSourceId();
    final SourceConnection updatedSource = configurationUpdate
        .source(sourceId, sourceUpdate.getName(),
            sourceUpdate.getConnectionConfiguration());
    final ConnectorSpecification spec = getSpecFromSourceId(sourceId);
    validateSource(spec, sourceUpdate.getConnectionConfiguration());

    ApmTraceUtils.addTagsToTrace(
        Map.of(
            WORKSPACE_ID, updatedSource.getWorkspaceId().toString(),
            SOURCE_ID, sourceId.toString()));

    // persist
    persistSourceConnection(
        updatedSource.getName(),
        updatedSource.getSourceDefinitionId(),
        updatedSource.getWorkspaceId(),
        updatedSource.getSourceId(),
        updatedSource.getTombstone(),
        updatedSource.getConfiguration(),
        spec,
        sourceUpdate.getResourceAllocation());

    // read configuration from db
    return buildSourceRead(sourceService.getSourceConnection(sourceId), spec);
  }

  /**
   * Upgrades the source to the source definition's default version.
   *
   * @param sourceIdRequestBody - ID of the source to upgrade
   */
  public void upgradeSourceVersion(final SourceIdRequestBody sourceIdRequestBody)
      throws IOException, JsonValidationException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceConnection sourceConnection = sourceService.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    actorDefinitionVersionUpdater.upgradeActorVersion(sourceConnection, sourceDefinition);
  }

  public SourceRead getSource(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return buildSourceRead(sourceIdRequestBody.getSourceId());
  }

  public ActorCatalogWithUpdatedAt getMostRecentSourceActorCatalogWithUpdatedAt(final SourceIdRequestBody sourceIdRequestBody)
      throws IOException {
    final Optional<io.airbyte.config.ActorCatalogWithUpdatedAt> actorCatalog =
        catalogService.getMostRecentSourceActorCatalog(sourceIdRequestBody.getSourceId());
    if (actorCatalog.isEmpty()) {
      return new ActorCatalogWithUpdatedAt();
    } else {
      return new ActorCatalogWithUpdatedAt().updatedAt(actorCatalog.get().getUpdatedAt()).catalog(actorCatalog.get().getCatalog());
    }
  }

  public SourceReadList listSourcesForWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final List<SourceConnection> sourceConnections = sourceService.listWorkspaceSourceConnection(workspaceIdRequestBody.getWorkspaceId());

    final List<SourceRead> reads = Lists.newArrayList();
    for (final SourceConnection sc : sourceConnections) {
      reads.add(buildSourceReadWithStatus(sc));
    }

    return new SourceReadList().sources(reads);
  }

  public SourceReadList listSourcesForWorkspaces(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final List<SourceConnection> sourceConnections =
        sourceService.listWorkspacesSourceConnections(new ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
            listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
            listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
            listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(), null));

    final List<SourceRead> reads = Lists.newArrayList();
    for (final SourceConnection sc : sourceConnections) {
      reads.add(buildSourceReadWithStatus(sc));
    }

    return new SourceReadList().sources(reads);
  }

  public SourceReadList listSourcesForSourceDefinition(final UUID sourceDefinitionId)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    final List<SourceRead> reads = Lists.newArrayList();
    for (final SourceConnection sourceConnection : sourceService.listSourcesForDefinition(sourceDefinitionId)) {
      reads.add(buildSourceRead(sourceConnection));
    }

    return new SourceReadList().sources(reads);
  }

  public SourceReadList searchSources(final SourceSearch sourceSearch)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final List<SourceRead> reads = Lists.newArrayList();

    for (final SourceConnection sci : sourceService.listSourceConnection()) {
      if (!sci.getTombstone()) {
        final SourceRead sourceRead = buildSourceRead(sci);
        if (MatchSearchHandler.matchSearch(sourceSearch, sourceRead)) {
          reads.add(sourceRead);
        }
      }
    }

    return new SourceReadList().sources(reads);
  }

  public void deleteSource(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    // get existing source
    final SourceRead source = buildSourceRead(sourceIdRequestBody.getSourceId());
    deleteSource(source);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public void deleteSource(final SourceRead source)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    // "delete" all connections associated with source as well.
    // Delete connections first in case it fails in the middle, source will still be visible
    final var workspaceIdRequestBody = new WorkspaceIdRequestBody()
        .workspaceId(source.getWorkspaceId());

    final List<UUID> uuidsToDelete = connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)
        .getConnections().stream()
        .filter(con -> con.getSourceId().equals(source.getSourceId()))
        .map(ConnectionRead::getConnectionId)
        .toList();

    for (final UUID uuidToDelete : uuidsToDelete) {
      connectionsHandler.deleteConnection(uuidToDelete);
    }

    final var spec = getSpecFromSourceId(source.getSourceId());

    // Delete secrets and config in this source and mark it tombstoned.
    try {
      sourceService.tombstoneSource(
          source.getName(),
          source.getWorkspaceId(),
          source.getSourceId(), spec);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  public DiscoverCatalogResult writeDiscoverCatalogResult(final SourceDiscoverSchemaWriteRequestBody request)
      throws JsonValidationException, IOException {
    final AirbyteCatalog persistenceCatalog = catalogConverter.toProtocol(request.getCatalog());
    final UUID catalogId = writeActorCatalog(persistenceCatalog, request);

    return new DiscoverCatalogResult().catalogId(catalogId);
  }

  private UUID writeActorCatalog(final AirbyteCatalog persistenceCatalog, final SourceDiscoverSchemaWriteRequestBody request) throws IOException {
    return catalogService.writeActorCatalogFetchEvent(
        persistenceCatalog,
        request.getSourceId(),
        request.getConnectorVersion(),
        request.getConfigurationHash());
  }

  private SourceRead buildSourceReadWithStatus(final SourceConnection sourceConnection)
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final SourceRead sourceRead = buildSourceRead(sourceConnection);
    // add source status into sourceRead
    if (sourceService.isSourceActive(sourceConnection.getSourceId())) {
      sourceRead.status(ActorStatus.ACTIVE);
    } else {
      sourceRead.status(ActorStatus.INACTIVE);
    }
    return sourceRead;
  }

  public SourceRead buildSourceRead(final UUID sourceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final SourceConnection sourceConnection = sourceService.getSourceConnection(sourceId);
    return buildSourceRead(sourceConnection);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId());
    final ConnectorSpecification spec = sourceVersion.getSpec();
    return buildSourceRead(sourceConnection, spec);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection, final ConnectorSpecification spec)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final StandardSourceDefinition standardSourceDefinition = sourceService
        .getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final JsonNode sanitizedConfig = secretsProcessor.prepareSecretsForOutput(sourceConnection.getConfiguration(), spec.getConnectionSpecification());
    sourceConnection.setConfiguration(sanitizedConfig);
    return toSourceRead(sourceConnection, standardSourceDefinition);
  }

  private void validateSource(final ConnectorSpecification spec, final JsonNode implementationJson)
      throws JsonValidationException {
    validator.ensure(spec.getConnectionSpecification(), implementationJson);
  }

  private ConnectorSpecification getSpecFromSourceId(final UUID sourceId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = sourceService.getSourceConnection(sourceId);
    final StandardSourceDefinition sourceDef = sourceService.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), sourceId);
    return sourceVersion.getSpec();
  }

  private ConnectorSpecification getSpecFromSourceDefinitionIdForWorkspace(final UUID sourceDefId, final UUID workspaceId)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardSourceDefinition sourceDef = sourceService.getStandardSourceDefinition(sourceDefId);
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, workspaceId);
    return sourceVersion.getSpec();
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  private void persistSourceConnection(final String name,
                                       final UUID sourceDefinitionId,
                                       final UUID workspaceId,
                                       final UUID sourceId,
                                       final boolean tombstone,
                                       final JsonNode configurationJson,
                                       final ConnectorSpecification spec,
                                       final io.airbyte.api.model.generated.ScopedResourceRequirements resourceRequirements)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId);
    licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.SOURCE_CONNECTOR, sourceDefinitionId);

    final JsonNode oAuthMaskedConfigurationJson =
        oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, configurationJson, spec);

    final SourceConnection sourceConnection = new SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withTombstone(tombstone)
        .withConfiguration(oAuthMaskedConfigurationJson)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements));
    try {
      sourceService.writeSourceConnectionWithSecrets(sourceConnection, spec);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  protected SourceRead toSourceRead(final SourceConnection sourceConnection,
                                    final StandardSourceDefinition standardSourceDefinition)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {

    final ActorDefinitionVersionWithOverrideStatus sourceVersionWithOverrideStatus = actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
        standardSourceDefinition, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId());

    final Optional<ActorDefinitionVersionBreakingChanges> breakingChanges =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(sourceVersionWithOverrideStatus.actorDefinitionVersion());

    final UUID organizationId = workspaceHelper.getOrganizationForWorkspace(sourceConnection.getWorkspaceId());
    final Boolean isEntitled =
        licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, standardSourceDefinition.getSourceDefinitionId());

    return new SourceRead()
        .sourceDefinitionId(standardSourceDefinition.getSourceDefinitionId())
        .sourceName(standardSourceDefinition.getName())
        .sourceId(sourceConnection.getSourceId())
        .workspaceId(sourceConnection.getWorkspaceId())
        .sourceDefinitionId(sourceConnection.getSourceDefinitionId())
        .connectionConfiguration(sourceConnection.getConfiguration())
        .name(sourceConnection.getName())
        .icon(standardSourceDefinition.getIconUrl())
        .isVersionOverrideApplied(sourceVersionWithOverrideStatus.isOverrideApplied())
        .isEntitled(isEntitled)
        .breakingChanges(breakingChanges.orElse(null))
        .supportState(apiPojoConverters.toApiSupportState(sourceVersionWithOverrideStatus.actorDefinitionVersion().getSupportState()))
        .createdAt(sourceConnection.getCreatedAt())
        .resourceAllocation(apiPojoConverters.scopedResourceReqsToApi(sourceConnection.getResourceRequirements()));
  }

  protected SourceSnippetRead toSourceSnippetRead(final SourceConnection source, final StandardSourceDefinition sourceDefinition) {
    return new SourceSnippetRead()
        .sourceId(source.getSourceId())
        .name(source.getName())
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId())
        .sourceName(sourceDefinition.getName())
        .icon(sourceDefinition.getIconUrl());
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode hydrateOAuthResponseSecret(final String secretId, final UUID workspaceId)
      throws ConfigNotFoundException, JsonValidationException, IOException {
    final SecretCoordinate secretCoordinate = SecretCoordinate.Companion.fromFullCoordinate(secretId);
    final UUID organizationId;
    try {
      organizationId = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false).getOrganizationId();
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final JsonNode secret;
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      final SecretPersistenceConfig secretPersistenceConfig;
      try {
        secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId);
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
      secret =
          secretsRepositoryReader.fetchSecretFromRuntimeSecretPersistence(secretCoordinate,
              new RuntimeSecretPersistence(secretPersistenceConfig, metricClient));
    } else {
      secret = secretsRepositoryReader.fetchSecretFromDefaultSecretPersistence(secretCoordinate);
    }
    final CompleteOAuthResponse completeOAuthResponse = Jsons.object(secret, CompleteOAuthResponse.class);
    return Jsons.jsonNode(completeOAuthResponse.getAuthPayload());
  }

}
