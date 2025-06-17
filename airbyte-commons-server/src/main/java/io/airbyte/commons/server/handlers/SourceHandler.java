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
import io.airbyte.commons.server.support.CurrentUserService;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.secrets.ConfigWithProcessedSecrets;
import io.airbyte.config.secrets.ConfigWithSecretReferences;
import io.airbyte.config.secrets.InlinedConfigWithSecretRefsKt;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.SecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.CatalogService;
import io.airbyte.data.services.PartialUserConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.domain.models.SecretStorage;
import io.airbyte.domain.services.secrets.SecretPersistenceService;
import io.airbyte.domain.services.secrets.SecretReferenceService;
import io.airbyte.domain.services.secrets.SecretStorageService;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.v0.AirbyteCatalog;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
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
  private final SourceService sourceService;
  private final WorkspaceHelper workspaceHelper;
  private final SecretPersistenceService secretPersistenceService;
  private final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private final LicenseEntitlementChecker licenseEntitlementChecker;
  private final CatalogConverter catalogConverter;
  private final ApiPojoConverters apiPojoConverters;
  private final Configs.AirbyteEdition airbyteEdition;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretStorageService secretStorageService;
  private final SecretReferenceService secretReferenceService;
  private final CurrentUserService currentUserService;
  private final PartialUserConfigService partialUserConfigService;

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
                       final SourceService sourceService,
                       final WorkspaceHelper workspaceHelper,
                       final SecretPersistenceService secretPersistenceService,
                       final ActorDefinitionHandlerHelper actorDefinitionHandlerHelper,
                       final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater,
                       final LicenseEntitlementChecker licenseEntitlementChecker,
                       final CatalogConverter catalogConverter,
                       final ApiPojoConverters apiPojoConverters,
                       final Configs.AirbyteEdition airbyteEdition,
                       final SecretsRepositoryWriter secretsRepositoryWriter,
                       final SecretStorageService secretStorageService,
                       final SecretReferenceService secretReferenceService,
                       final CurrentUserService currentUserService,
                       final PartialUserConfigService partialUserConfigService) {
    this.catalogService = catalogService;
    this.secretsRepositoryReader = secretsRepositoryReader;
    validator = integrationSchemaValidation;
    this.connectionsHandler = connectionsHandler;
    this.uuidGenerator = uuidGenerator;
    this.configurationUpdate = configurationUpdate;
    this.secretsProcessor = secretsProcessor;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.sourceService = sourceService;
    this.workspaceHelper = workspaceHelper;
    this.secretPersistenceService = secretPersistenceService;
    this.actorDefinitionHandlerHelper = actorDefinitionHandlerHelper;
    this.actorDefinitionVersionUpdater = actorDefinitionVersionUpdater;
    this.licenseEntitlementChecker = licenseEntitlementChecker;
    this.catalogConverter = catalogConverter;
    this.apiPojoConverters = apiPojoConverters;
    this.airbyteEdition = airbyteEdition;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretStorageService = secretStorageService;
    this.secretReferenceService = secretReferenceService;
    this.currentUserService = currentUserService;
    this.partialUserConfigService = partialUserConfigService;
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

    validateSourceUpdate(partialSourceUpdate.getConnectionConfiguration(), updatedSource, spec);

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
        .source(sourceId, sourceUpdate.getName(), sourceUpdate.getConnectionConfiguration());
    final ConnectorSpecification spec = getSpecFromSourceId(sourceId);

    validateSourceUpdate(sourceUpdate.getConnectionConfiguration(), updatedSource, spec);

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
    return getSource(sourceIdRequestBody, false);
  }

  public SourceRead getSource(final SourceIdRequestBody sourceIdRequestBody, Boolean includeSecretCoordinates)
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    return buildSourceRead(sourceIdRequestBody.getSourceId(), includeSecretCoordinates);
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

    final SecretPersistence secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceIdRequestBody.getWorkspaceId());

    final ConfigWithSecretReferences configWithSecretReferences = secretReferenceService.getConfigWithSecretReferences(
        source.getSourceId(),
        source.getConnectionConfiguration(),
        source.getWorkspaceId());

    // Delete airbyte-managed secrets for this source
    secretsRepositoryWriter.deleteFromConfig(configWithSecretReferences, secretPersistence);

    // Delete secret references for this source
    secretReferenceService.deleteActorSecretReferences(source.getSourceId());

    // Delete partial user config(s) for this source, if any
    partialUserConfigService.deletePartialUserConfigForSource(source.getSourceId());

    // Mark source as tombstoned and clear config
    try {
      sourceService.tombstoneSource(
          source.getName(),
          source.getWorkspaceId(),
          source.getSourceId());
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
    return catalogService.writeActorCatalogWithFetchEvent(
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
    return buildSourceRead(sourceId, false);
  }

  public SourceRead buildSourceRead(final UUID sourceId, final Boolean includeSecretCoordinates)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final SourceConnection sourceConnection = sourceService.getSourceConnection(sourceId);
    return buildSourceRead(sourceConnection, includeSecretCoordinates);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return buildSourceRead(sourceConnection, false);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection, final Boolean includeSecretCoordinates)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = sourceService.getSourceDefinitionFromSource(sourceConnection.getSourceId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConnection.getWorkspaceId(), sourceConnection.getSourceId());
    final ConnectorSpecification spec = sourceVersion.getSpec();
    return buildSourceRead(sourceConnection, spec, includeSecretCoordinates);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection, final ConnectorSpecification spec)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    return buildSourceRead(sourceConnection, spec, false);
  }

  private SourceRead buildSourceRead(final SourceConnection sourceConnection,
                                     final ConnectorSpecification spec,
                                     final Boolean includeSecretCoordinates)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    // read configuration from db
    final StandardSourceDefinition standardSourceDefinition = sourceService
        .getStandardSourceDefinition(sourceConnection.getSourceDefinitionId());
    final UUID organizationId = workspaceHelper.getOrganizationForWorkspace(sourceConnection.getWorkspaceId());
    if (includeSecretCoordinates
        && !this.licenseEntitlementChecker.checkEntitlements(organizationId, Entitlement.ACTOR_CONFIG_WITH_SECRET_COORDINATES)) {
      throw new IllegalArgumentException("ACTOR_CONFIG_WITH_SECRET_COORDINATES not entitled for this organization");
    }
    final ConfigWithSecretReferences configWithRefs =
        this.secretReferenceService.getConfigWithSecretReferences(sourceConnection.getSourceId(), sourceConnection.getConfiguration(),
            sourceConnection.getWorkspaceId());
    final JsonNode inlinedConfigWithRefs = InlinedConfigWithSecretRefsKt.toInlined(configWithRefs);
    final JsonNode sanitizedConfig =
        includeSecretCoordinates
            ? secretsProcessor.simplifySecretsForOutput(inlinedConfigWithRefs, spec.getConnectionSpecification())
            : secretsProcessor.prepareSecretsForOutput(inlinedConfigWithRefs, spec.getConnectionSpecification());
    sourceConnection.setConfiguration(sanitizedConfig);
    return toSourceRead(sourceConnection, standardSourceDefinition);
  }

  private void validateSource(final ConnectorSpecification spec, final JsonNode implementationJson)
      throws JsonValidationException {
    validator.ensure(spec.getConnectionSpecification(), implementationJson);
  }

  /**
   * Validates the provided update JSON against the spec by merging it into the full updated source
   * config.
   * <p>
   * Note: The existing source config may have been persisted with secret object nodes instead of raw
   * values, which must be replaced with placeholder text nodes in order to pass validation.
   */
  private void validateSourceUpdate(final JsonNode providedUpdateJson, final SourceConnection updatedSource, final ConnectorSpecification spec)
      throws JsonValidationException {
    // Replace any secret object nodes with placeholder text nodes that will pass validation.
    final JsonNode updatedSourceConfigWithSecretPlaceholders =
        SecretReferenceHelpers.INSTANCE.configWithTextualSecretPlaceholders(
            updatedSource.getConfiguration(),
            spec.getConnectionSpecification());
    // Merge the provided update JSON into the updated source config with secret placeholders.
    // The final result should pass validation as long as the provided update JSON is valid.
    final JsonNode mergedConfig = Optional.ofNullable(providedUpdateJson)
        .map(update -> Jsons.mergeNodes(updatedSourceConfigWithSecretPlaceholders, update))
        .orElse(updatedSourceConfigWithSecretPlaceholders);

    validateSource(spec, mergedConfig);
  }

  private ConnectorSpecification getSpecFromSourceId(final UUID sourceId)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final SourceConnection source = sourceService.getSourceConnection(sourceId);
    final StandardSourceDefinition sourceDef = sourceService.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), sourceId);
    return sourceVersion.getSpec();
  }

  public ConnectorSpecification getSpecFromSourceDefinitionIdForWorkspace(final UUID sourceDefId, final UUID workspaceId)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardSourceDefinition sourceDef = sourceService.getStandardSourceDefinition(sourceDefId);
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, workspaceId);
    return sourceVersion.getSpec();
  }

  /**
   * Persists a source to the database, with secret masking and handling applied for OAuth and other
   * secret values in the provided configuration json. Raw secret values and prefixed secret
   * coordinates are split out from the provided config and replaced with coordinates or secret
   * reference IDs, depending on whether the workspace has a secret storage configured.
   */
  private void persistSourceConnection(final String name,
                                       final UUID sourceDefinitionId,
                                       final UUID workspaceId,
                                       final UUID sourceId,
                                       final boolean tombstone,
                                       final JsonNode configurationJson,
                                       final ConnectorSpecification spec,
                                       final io.airbyte.api.model.generated.ScopedResourceRequirements resourceRequirements)
      throws JsonValidationException, IOException {
    final UUID organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId);
    licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.SOURCE_CONNECTOR, sourceDefinitionId);

    final JsonNode maskedConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, configurationJson, spec);
    final Optional<UUID> secretStorageId = Optional.ofNullable(secretStorageService.getByWorkspaceId(workspaceId)).map(SecretStorage::getIdJava);

    final SourceConnection newSourceConnection = new SourceConnection()
        .withName(name)
        .withSourceDefinitionId(sourceDefinitionId)
        .withWorkspaceId(workspaceId)
        .withSourceId(sourceId)
        .withTombstone(tombstone)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements));

    JsonNode updatedConfig = persistConfigRawSecretValues(maskedConfig, secretStorageId, workspaceId, spec, sourceId);

    if (secretStorageId.isPresent()) {
      final ConfigWithProcessedSecrets reprocessedConfig = SecretReferenceHelpers.INSTANCE.processConfigSecrets(
          updatedConfig, spec.getConnectionSpecification(), secretStorageId.orElse(null));
      updatedConfig = secretReferenceService.createAndInsertSecretReferencesWithStorageId(
          reprocessedConfig,
          sourceId,
          workspaceId,
          secretStorageId.get(),
          currentUserService.getCurrentUserIdIfExists().orElse(null));
    }

    newSourceConnection.setConfiguration(updatedConfig);
    sourceService.writeSourceConnectionNoSecrets(newSourceConnection);
  }

  /**
   * Persists raw secret values for the given config. Creates or updates depending on whether a prior
   * config exists.
   *
   * @return new config with secret values replaced with secret coordinate nodes.
   */
  public JsonNode persistConfigRawSecretValues(final JsonNode config,
                                               final Optional<UUID> secretStorageId,
                                               final UUID workspaceId,
                                               final ConnectorSpecification spec,
                                               final UUID sourceId)
      throws JsonValidationException {
    final SecretPersistence secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId);
    final ConfigWithProcessedSecrets processedConfig = SecretReferenceHelpers.INSTANCE.processConfigSecrets(
        config, spec.getConnectionSpecification(), secretStorageId.orElse(null));
    final Optional<JsonNode> previousConfig = sourceService.getSourceConnectionIfExists(sourceId)
        .map(SourceConnection::getConfiguration);
    if (previousConfig.isPresent()) {
      final ConfigWithSecretReferences priorConfigWithSecretReferences =
          secretReferenceService.getConfigWithSecretReferences(
              sourceId,
              previousConfig.get(),
              workspaceId);
      return secretsRepositoryWriter.updateFromConfig(
          workspaceId,
          priorConfigWithSecretReferences,
          processedConfig,
          spec.getConnectionSpecification(),
          secretPersistence);
    } else {
      return secretsRepositoryWriter.createFromConfig(
          workspaceId,
          processedConfig,
          secretPersistence);
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
  JsonNode hydrateOAuthResponseSecret(final String secretId, final UUID workspaceId) {
    final SecretCoordinate secretCoordinate = SecretCoordinate.Companion.fromFullCoordinate(secretId);
    final SecretPersistence secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId);
    final JsonNode secret = secretsRepositoryReader.fetchSecretFromSecretPersistence(secretCoordinate, secretPersistence);
    final CompleteOAuthResponse completeOAuthResponse = Jsons.object(secret, CompleteOAuthResponse.class);
    return Jsons.jsonNode(completeOAuthResponse.getAuthPayload());
  }

}
