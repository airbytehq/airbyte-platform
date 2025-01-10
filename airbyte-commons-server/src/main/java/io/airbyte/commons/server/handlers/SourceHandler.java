/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt;
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DiscoverCatalogResult;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.PartialSourceUpdate;
import io.airbyte.api.model.generated.SourceCloneConfiguration;
import io.airbyte.api.model.generated.SourceCloneRequestBody;
import io.airbyte.api.model.generated.SourceCreate;
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody;
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.SourceSearch;
import io.airbyte.api.model.generated.SourceSnippetRead;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper;
import io.airbyte.config.ActorDefinitionVersion;
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
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
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
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final CatalogConverter catalogConverter;
  private final ApiPojoConverters apiPojoConverters;

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
                       final SecretPersistenceConfigService secretPersistenceConfigService,
                       final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                       final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater,
                       final CatalogConverter catalogConverter,
                       final ApiPojoConverters apiPojoConverters) {
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
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater;
    this.catalogConverter = catalogConverter;
    this.apiPojoConverters = apiPojoConverters;
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
  public SourceRead updateSourceWithOptionalSecret(final PartialSourceUpdate partialSourceUpdate)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final ConnectorSpecification spec = getSpecFromSourceId(partialSourceUpdate.getSourceId());
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
        spec);

    // read configuration from db
    return buildSourceRead(sourceService.getSourceConnection(sourceId), spec);
  }

  public SourceRead partialUpdateSource(final PartialSourceUpdate partialSourceUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {

    final UUID sourceId = partialSourceUpdate.getSourceId();
    final SourceConnection updatedSource = configurationUpdate
        .partialSource(sourceId, partialSourceUpdate.getName(),
            partialSourceUpdate.getConnectionConfiguration());
    final ConnectorSpecification spec = getSpecFromSourceId(sourceId);
    validateSource(spec, updatedSource.getConfiguration());

    // persist
    persistSourceConnection(
        updatedSource.getName(),
        updatedSource.getSourceDefinitionId(),
        updatedSource.getWorkspaceId(),
        updatedSource.getSourceId(),
        updatedSource.getTombstone(),
        updatedSource.getConfiguration(),
        spec);

    // read configuration from db
    return buildSourceRead(sourceService.getSourceConnection(sourceId), spec);
  }

  public SourceRead updateSource(final SourceUpdate sourceUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException, io.airbyte.config.persistence.ConfigNotFoundException {

    final UUID sourceId = sourceUpdate.getSourceId();
    final SourceConnection updatedSource = configurationUpdate
        .source(sourceId, sourceUpdate.getName(),
            sourceUpdate.getConnectionConfiguration());
    final ConnectorSpecification spec = getSpecFromSourceId(sourceId);
    validateSource(spec, sourceUpdate.getConnectionConfiguration());

    // persist
    persistSourceConnection(
        updatedSource.getName(),
        updatedSource.getSourceDefinitionId(),
        updatedSource.getWorkspaceId(),
        updatedSource.getSourceId(),
        updatedSource.getTombstone(),
        updatedSource.getConfiguration(),
        spec);

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
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
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

  public SourceRead cloneSource(final SourceCloneRequestBody sourceCloneRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    // read source configuration from db
    final SourceRead sourceToClone;
    sourceToClone = buildSourceReadWithSecrets(sourceCloneRequestBody.getSourceCloneId());
    final SourceCloneConfiguration sourceCloneConfiguration = sourceCloneRequestBody.getSourceConfiguration();

    final String copyText = " (Copy)";
    final String sourceName = sourceToClone.getName() + copyText;

    final SourceCreate sourceCreate = new SourceCreate()
        .name(sourceName)
        .sourceDefinitionId(sourceToClone.getSourceDefinitionId())
        .connectionConfiguration(sourceToClone.getConnectionConfiguration())
        .workspaceId(sourceToClone.getWorkspaceId());

    if (sourceCloneConfiguration != null) {
      if (sourceCloneConfiguration.getName() != null) {
        sourceCreate.name(sourceCloneConfiguration.getName());
      }

      if (sourceCloneConfiguration.getConnectionConfiguration() != null) {
        sourceCreate.connectionConfiguration(sourceCloneConfiguration.getConnectionConfiguration());
      }
    }

    return createSource(sourceCreate);
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

  public SourceReadList listSourcesForSourceDefinition(final SourceDefinitionIdRequestBody sourceDefinitionIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    final List<SourceRead> reads = Lists.newArrayList();
    for (final SourceConnection sourceConnection : sourceService.listSourcesForDefinition(sourceDefinitionIdRequestBody.getSourceDefinitionId())) {
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

  @SuppressWarnings("PMD.PreserveStackTrace")
  private SourceRead buildSourceReadWithSecrets(final UUID sourceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final SourceConnection sourceConnection;
    try {
      sourceConnection = sourceService.getSourceConnectionWithSecrets(sourceId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final StandardSourceDefinition standardSourceDefinition = sourceService
        .getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
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
                                       final ConnectorSpecification spec)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final JsonNode oAuthMaskedConfigurationJson =
        oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, configurationJson, spec);
    final SourceConnection sourceConnection = new SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withTombstone(tombstone)
        .withConfiguration(oAuthMaskedConfigurationJson);
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
        .breakingChanges(breakingChanges.orElse(null))
        .supportState(apiPojoConverters.toApiSupportState(sourceVersionWithOverrideStatus.actorDefinitionVersion().getSupportState()))
        .createdAt(sourceConnection.getCreatedAt());
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
          secretsRepositoryReader.fetchSecretFromRuntimeSecretPersistence(secretCoordinate, new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      secret = secretsRepositoryReader.fetchSecretFromDefaultSecretPersistence(secretCoordinate);
    }
    final CompleteOAuthResponse completeOAuthResponse = Jsons.object(secret, CompleteOAuthResponse.class);
    return Jsons.jsonNode(completeOAuthResponse.getAuthPayload());
  }

}
