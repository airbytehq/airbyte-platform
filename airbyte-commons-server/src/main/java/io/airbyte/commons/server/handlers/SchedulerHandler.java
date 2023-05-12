/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.getUpdatedSchema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airbyte.api.model.generated.AdvancedAuth;
import io.airbyte.api.model.generated.AuthSpecification;
import io.airbyte.api.model.generated.CatalogDiff;
import io.airbyte.api.model.generated.CheckConnectionRead;
import io.airbyte.api.model.generated.CheckConnectionRead.StatusEnum;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.ConnectionStatus;
import io.airbyte.api.model.generated.ConnectionStream;
import io.airbyte.api.model.generated.ConnectionStreamRequestBody;
import io.airbyte.api.model.generated.ConnectionUpdate;
import io.airbyte.api.model.generated.DestinationCoreConfig;
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.FieldTransform;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.LogRead;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.model.generated.SourceCoreConfig;
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId;
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.StreamTransform.TransformTypeEnum;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.converters.OauthModelConverter;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.temporal.ErrorCode;
import io.airbyte.commons.temporal.TemporalClient.ManualOperationResult;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.SecretsRepositoryReader;
import io.airbyte.config.persistence.SecretsRepositoryWriter;
import io.airbyte.featureflag.AutoPropagateSchema;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

/**
 * ScheduleHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("MissingJavadocMethod")
@Singleton
@Slf4j
public class SchedulerHandler {

  private static final HashFunction HASH_FUNCTION = Hashing.md5();

  private static final ImmutableSet<ErrorCode> VALUE_CONFLICT_EXCEPTION_ERROR_CODE_SET =
      ImmutableSet.of(ErrorCode.WORKFLOW_DELETED, ErrorCode.WORKFLOW_RUNNING);

  private final ConnectionsHandler connectionsHandler;
  private final ConfigRepository configRepository;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SynchronousSchedulerClient synchronousSchedulerClient;
  private final ConfigurationUpdate configurationUpdate;
  private final JsonSchemaValidator jsonSchemaValidator;
  private final JobPersistence jobPersistence;
  private final JobConverter jobConverter;
  private final EventRunner eventRunner;
  private final FeatureFlags envVariableFeatureFlags;
  private final WebUrlHelper webUrlHelper;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final FeatureFlagClient featureFlagClient;

  // TODO: Convert to be fully using micronaut
  public SchedulerHandler(final ConfigRepository configRepository,
                          final SecretsRepositoryReader secretsRepositoryReader,
                          final SecretsRepositoryWriter secretsRepositoryWriter,
                          final SynchronousSchedulerClient synchronousSchedulerClient,
                          final JobPersistence jobPersistence,
                          final WorkerEnvironment workerEnvironment,
                          final LogConfigs logConfigs,
                          final EventRunner eventRunner,
                          final ConnectionsHandler connectionsHandler,
                          final FeatureFlags envVariableFeatureFlags,
                          final WebUrlHelper webUrlHelper,
                          final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                          final FeatureFlagClient featureFlagClient) {
    this(
        configRepository,
        secretsRepositoryWriter,
        synchronousSchedulerClient,
        new ConfigurationUpdate(configRepository, secretsRepositoryReader, actorDefinitionVersionHelper),
        new JsonSchemaValidator(),
        jobPersistence,
        eventRunner,
        new JobConverter(workerEnvironment, logConfigs),
        connectionsHandler,
        envVariableFeatureFlags,
        webUrlHelper,
        actorDefinitionVersionHelper,
        featureFlagClient);
  }

  @VisibleForTesting
  SchedulerHandler(final ConfigRepository configRepository,
                   final SecretsRepositoryWriter secretsRepositoryWriter,
                   final SynchronousSchedulerClient synchronousSchedulerClient,
                   final ConfigurationUpdate configurationUpdate,
                   final JsonSchemaValidator jsonSchemaValidator,
                   final JobPersistence jobPersistence,
                   final EventRunner eventRunner,
                   final JobConverter jobConverter,
                   final ConnectionsHandler connectionsHandler,
                   final FeatureFlags envVariableFeatureFlags,
                   final WebUrlHelper webUrlHelper,
                   final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                   final FeatureFlagClient featureFlagClient) {
    this.configRepository = configRepository;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.synchronousSchedulerClient = synchronousSchedulerClient;
    this.configurationUpdate = configurationUpdate;
    this.jsonSchemaValidator = jsonSchemaValidator;
    this.jobPersistence = jobPersistence;
    this.eventRunner = eventRunner;
    this.jobConverter = jobConverter;
    this.connectionsHandler = connectionsHandler;
    this.envVariableFeatureFlags = envVariableFeatureFlags;
    this.webUrlHelper = webUrlHelper;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.featureFlagClient = featureFlagClient;
  }

  public CheckConnectionRead checkSourceConnectionFromSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID sourceId = sourceIdRequestBody.getSourceId();
    final SourceConnection source = configRepository.getSourceConnection(sourceId);
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), sourceId);
    final String imageName = sourceVersion.getDockerRepository() + ":" + sourceVersion.getDockerImageTag();
    final boolean isCustomConnector = sourceDef.getCustom();
    final Version protocolVersion = new Version(sourceDef.getProtocolVersion());

    return reportConnectionStatus(synchronousSchedulerClient.createSourceCheckConnectionJob(source, imageName, protocolVersion, isCustomConnector));
  }

  public CheckConnectionRead checkSourceConnectionFromSourceCreate(final SourceCoreConfig sourceConfig)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(sourceConfig.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConfig.getWorkspaceId(), sourceConfig.getSourceId());
    final var partialConfig = secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        sourceConfig.getConnectionConfiguration(),
        sourceVersion.getSpec());

    // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
    // technically declared as required.
    final SourceConnection source = new SourceConnection()
        .withSourceId(sourceConfig.getSourceId())
        .withSourceDefinitionId(sourceConfig.getSourceDefinitionId())
        .withConfiguration(partialConfig)
        .withWorkspaceId(sourceConfig.getWorkspaceId());

    final Version protocolVersion = new Version(sourceDef.getProtocolVersion());

    final String imageName = sourceVersion.getDockerRepository() + ":" + sourceVersion.getDockerImageTag();
    final boolean isCustomConnector = sourceDef.getCustom();
    return reportConnectionStatus(synchronousSchedulerClient.createSourceCheckConnectionJob(source, imageName, protocolVersion, isCustomConnector));
  }

  public CheckConnectionRead checkSourceConnectionFromSourceIdForUpdate(final SourceUpdate sourceUpdate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final SourceConnection updatedSource =
        configurationUpdate.source(sourceUpdate.getSourceId(), sourceUpdate.getName(), sourceUpdate.getConnectionConfiguration());

    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(updatedSource.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, updatedSource.getWorkspaceId(), updatedSource.getSourceId());
    jsonSchemaValidator.ensure(sourceVersion.getSpec().getConnectionSpecification(), updatedSource.getConfiguration());

    final SourceCoreConfig sourceCoreConfig = new SourceCoreConfig()
        .sourceId(updatedSource.getSourceId())
        .connectionConfiguration(updatedSource.getConfiguration())
        .sourceDefinitionId(updatedSource.getSourceDefinitionId())
        .workspaceId(updatedSource.getWorkspaceId());

    return checkSourceConnectionFromSourceCreate(sourceCoreConfig);
  }

  public CheckConnectionRead checkDestinationConnectionFromDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final DestinationConnection destination = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDef = configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDef, destination.getWorkspaceId(), destination.getDestinationId());
    final String imageName = destinationVersion.getDockerRepository() + ":" + destinationVersion.getDockerImageTag();
    final boolean isCustomConnector = destinationDef.getCustom();
    final Version protocolVersion = new Version(destinationDef.getProtocolVersion());
    return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, imageName, protocolVersion, isCustomConnector));
  }

  public CheckConnectionRead checkDestinationConnectionFromDestinationCreate(final DestinationCoreConfig destinationConfig)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destDef = configRepository.getStandardDestinationDefinition(destinationConfig.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destDef, destinationConfig.getWorkspaceId(), destinationConfig.getDestinationId());
    final var partialConfig = secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        destinationConfig.getConnectionConfiguration(),
        destinationVersion.getSpec());
    final boolean isCustomConnector = destDef.getCustom();

    // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
    // technically declared as required.
    final DestinationConnection destination = new DestinationConnection()
        .withDestinationId(destinationConfig.getDestinationId())
        .withDestinationDefinitionId(destinationConfig.getDestinationDefinitionId())
        .withConfiguration(partialConfig)
        .withWorkspaceId(destinationConfig.getWorkspaceId());
    final String imageName = destinationVersion.getDockerRepository() + ":" + destinationVersion.getDockerImageTag();
    final Version protocolVersion = new Version(destDef.getProtocolVersion());
    return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, imageName, protocolVersion, isCustomConnector));
  }

  public CheckConnectionRead checkDestinationConnectionFromDestinationIdForUpdate(final DestinationUpdate destinationUpdate)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final DestinationConnection updatedDestination = configurationUpdate
        .destination(destinationUpdate.getDestinationId(), destinationUpdate.getName(), destinationUpdate.getConnectionConfiguration());

    final StandardDestinationDefinition destinationDef =
        configRepository.getStandardDestinationDefinition(updatedDestination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDef,
        updatedDestination.getWorkspaceId(), updatedDestination.getDestinationId());
    jsonSchemaValidator.ensure(destinationVersion.getSpec().getConnectionSpecification(), updatedDestination.getConfiguration());

    final DestinationCoreConfig destinationCoreConfig = new DestinationCoreConfig()
        .destinationId(updatedDestination.getDestinationId())
        .connectionConfiguration(updatedDestination.getConfiguration())
        .destinationDefinitionId(updatedDestination.getDestinationDefinitionId())
        .workspaceId(updatedDestination.getWorkspaceId());

    return checkDestinationConnectionFromDestinationCreate(destinationCoreConfig);
  }

  public SourceDiscoverSchemaRead discoverSchemaForSourceFromSourceId(final SourceDiscoverSchemaRequestBody discoverSchemaRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID sourceId = discoverSchemaRequestBody.getSourceId();
    final SourceConnection source = configRepository.getSourceConnection(sourceId);
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), sourceId);
    final String imageName = sourceVersion.getDockerRepository() + ":" + sourceVersion.getDockerImageTag();
    final boolean isCustomConnector = sourceDef.getCustom();

    final String configHash = HASH_FUNCTION.hashBytes(Jsons.serialize(source.getConfiguration()).getBytes(
        Charsets.UTF_8)).toString();
    final String connectorVersion = sourceVersion.getDockerImageTag();
    final Optional<ActorCatalog> currentCatalog =
        configRepository.getActorCatalog(discoverSchemaRequestBody.getSourceId(), connectorVersion, configHash);
    final boolean bustActorCatalogCache = discoverSchemaRequestBody.getDisableCache() != null && discoverSchemaRequestBody.getDisableCache();
    if (currentCatalog.isEmpty() || bustActorCatalogCache) {
      final SynchronousResponse<UUID> persistedCatalogId =
          synchronousSchedulerClient.createDiscoverSchemaJob(
              source,
              imageName,
              connectorVersion,
              new Version(sourceDef.getProtocolVersion()),
              isCustomConnector);
      final SourceDiscoverSchemaRead discoveredSchema = retrieveDiscoveredSchema(persistedCatalogId, sourceDef);

      if (persistedCatalogId.isSuccess() && discoverSchemaRequestBody.getConnectionId() != null) {
        // modify discoveredSchema object to add CatalogDiff, containsBreakingChange, and connectionStatus
        generateCatalogDiffsAndDisableConnectionsIfNeeded(discoveredSchema, discoverSchemaRequestBody, source.getWorkspaceId());
      }

      return discoveredSchema;
    }
    final AirbyteCatalog airbyteCatalog = Jsons.object(currentCatalog.get().getCatalog(), AirbyteCatalog.class);
    final SynchronousJobRead emptyJob = new SynchronousJobRead()
        .configId("NoConfiguration")
        .configType(JobConfigType.DISCOVER_SCHEMA)
        .id(UUID.randomUUID())
        .createdAt(0L)
        .endedAt(0L)
        .logs(new LogRead().logLines(new ArrayList<>()))
        .succeeded(true);
    return new SourceDiscoverSchemaRead()
        .catalog(CatalogConverter.toApi(airbyteCatalog, sourceDef))
        .jobInfo(emptyJob)
        .catalogId(currentCatalog.get().getId());
  }

  public void applySchemaChangeForSource(final SourceAutoPropagateChange sourceAutoPropagateChange)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    final ConnectionReadList connectionsForSource =
        connectionsHandler.listConnectionsForSource(sourceAutoPropagateChange.getSourceId(), false);
    for (final ConnectionRead connectionRead : connectionsForSource.getConnections()) {
      final Optional<io.airbyte.api.model.generated.AirbyteCatalog> catalogUsedToMakeConfiguredCatalog = connectionsHandler
          .getConnectionAirbyteCatalog(connectionRead.getConnectionId());
      final io.airbyte.api.model.generated.@NotNull AirbyteCatalog currentAirbyteCatalog =
          connectionRead.getSyncCatalog();
      final CatalogDiff diff =
          connectionsHandler.getDiff(catalogUsedToMakeConfiguredCatalog.orElse(currentAirbyteCatalog),
              sourceAutoPropagateChange.getCatalog(),
              CatalogConverter.toConfiguredProtocol(currentAirbyteCatalog));

      final ConnectionUpdate updateObject =
          new ConnectionUpdate().connectionId(connectionRead.getConnectionId());

      if (shouldAutoPropagate(diff, sourceAutoPropagateChange.getWorkspaceId(), connectionRead)) {
        applySchemaChange(sourceAutoPropagateChange.getWorkspaceId(),
            updateObject,
            currentAirbyteCatalog,
            sourceAutoPropagateChange.getCatalog(),
            diff.getTransforms(),
            sourceAutoPropagateChange.getCatalogId());
      }
      connectionsHandler.updateConnection(updateObject);
    }
  }

  public SourceDiscoverSchemaRead discoverSchemaForSourceFromSourceCreate(final SourceCoreConfig sourceCreate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(sourceCreate.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceCreate.getWorkspaceId(), sourceCreate.getSourceId());
    final var partialConfig = secretsRepositoryWriter.statefulSplitEphemeralSecrets(
        sourceCreate.getConnectionConfiguration(),
        sourceVersion.getSpec());

    final String imageName = sourceVersion.getDockerRepository() + ":" + sourceVersion.getDockerImageTag();
    final boolean isCustomConnector = sourceDef.getCustom();
    // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
    // technically declared as required.
    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(sourceCreate.getSourceDefinitionId())
        .withConfiguration(partialConfig)
        .withWorkspaceId(sourceCreate.getWorkspaceId());
    final SynchronousResponse<UUID> response = synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        imageName,
        sourceVersion.getDockerImageTag(),
        new Version(
            sourceDef.getProtocolVersion()),
        isCustomConnector);
    return retrieveDiscoveredSchema(response, sourceDef);
  }

  private SourceDiscoverSchemaRead retrieveDiscoveredSchema(final SynchronousResponse<UUID> response, final StandardSourceDefinition sourceDef)
      throws ConfigNotFoundException, IOException {
    final SourceDiscoverSchemaRead sourceDiscoverSchemaRead = new SourceDiscoverSchemaRead()
        .jobInfo(jobConverter.getSynchronousJobRead(response));

    if (response.isSuccess()) {
      final ActorCatalog catalog = configRepository.getActorCatalogById(response.getOutput());
      final AirbyteCatalog persistenceCatalog = Jsons.object(catalog.getCatalog(),
          io.airbyte.protocol.models.AirbyteCatalog.class);
      sourceDiscoverSchemaRead.catalog(CatalogConverter.toApi(persistenceCatalog, sourceDef));
      sourceDiscoverSchemaRead.catalogId(response.getOutput());
    }

    return sourceDiscoverSchemaRead;
  }

  public SourceDefinitionSpecificationRead getSpecificationForSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final SourceConnection source = configRepository.getSourceConnection(sourceIdRequestBody.getSourceId());
    final StandardSourceDefinition sourceDefinition = configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.getWorkspaceId(), sourceIdRequestBody.getSourceId());
    final ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(sourceDefinition, spec);
  }

  public SourceDefinitionSpecificationRead getSourceDefinitionSpecification(final SourceDefinitionIdWithWorkspaceId sourceDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID sourceDefinitionId = sourceDefinitionIdWithWorkspaceId.getSourceDefinitionId();
    final StandardSourceDefinition source = configRepository.getStandardSourceDefinition(sourceDefinitionId);
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(source, sourceDefinitionIdWithWorkspaceId.getWorkspaceId());
    final ConnectorSpecification spec = sourceVersion.getSpec();

    return getSourceSpecificationRead(source, spec);
  }

  private SourceDefinitionSpecificationRead getSourceSpecificationRead(final StandardSourceDefinition sourceDefinition,
                                                                       final ConnectorSpecification spec) {
    final SourceDefinitionSpecificationRead specRead = new SourceDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(ConfigType.GET_SPEC)))
        .connectionSpecification(spec.getConnectionSpecification())
        .sourceDefinitionId(sourceDefinition.getSourceDefinitionId());

    if (spec.getDocumentationUrl() != null) {
      specRead.documentationUrl(spec.getDocumentationUrl().toString());
    }

    final Optional<AuthSpecification> authSpec = OauthModelConverter.getAuthSpec(spec);
    authSpec.ifPresent(specRead::setAuthSpecification);

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);

    return specRead;
  }

  public DestinationDefinitionSpecificationRead getSpecificationForDestinationId(final DestinationIdRequestBody destinationIdRequestBody)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final DestinationConnection destination = configRepository.getDestinationConnection(destinationIdRequestBody.getDestinationId());
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, destination.getWorkspaceId(),
            destinationIdRequestBody.getDestinationId());
    final ConnectorSpecification spec = destinationVersion.getSpec();
    return getDestinationSpecificationRead(destinationDefinition, spec);
  }

  @SuppressWarnings("LineLength")
  public DestinationDefinitionSpecificationRead getDestinationSpecification(
                                                                            final DestinationDefinitionIdWithWorkspaceId destinationDefinitionIdWithWorkspaceId)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID destinationDefinitionId = destinationDefinitionIdWithWorkspaceId.getDestinationDefinitionId();
    final StandardDestinationDefinition destination = configRepository.getStandardDestinationDefinition(destinationDefinitionId);
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destination, destinationDefinitionIdWithWorkspaceId.getWorkspaceId());
    final ConnectorSpecification spec = destinationVersion.getSpec();

    return getDestinationSpecificationRead(destination, spec);
  }

  private DestinationDefinitionSpecificationRead getDestinationSpecificationRead(final StandardDestinationDefinition destinationDefinition,
                                                                                 final ConnectorSpecification spec) {
    final DestinationDefinitionSpecificationRead specRead = new DestinationDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(SynchronousJobMetadata.mock(ConfigType.GET_SPEC)))
        .supportedDestinationSyncModes(Enums.convertListTo(spec.getSupportedDestinationSyncModes(), DestinationSyncMode.class))
        .connectionSpecification(spec.getConnectionSpecification())
        .documentationUrl(spec.getDocumentationUrl().toString())
        .destinationDefinitionId(destinationDefinition.getDestinationDefinitionId());

    final Optional<AuthSpecification> authSpec = OauthModelConverter.getAuthSpec(spec);
    authSpec.ifPresent(specRead::setAuthSpecification);

    final Optional<AdvancedAuth> advancedAuth = OauthModelConverter.getAdvancedAuth(spec);
    advancedAuth.ifPresent(specRead::setAdvancedAuth);

    return specRead;
  }

  public JobInfoRead syncConnection(final ConnectionIdRequestBody connectionIdRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    return submitManualSyncToWorker(connectionIdRequestBody.getConnectionId());
  }

  public JobInfoRead resetConnection(final ConnectionIdRequestBody connectionIdRequestBody)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    return submitResetConnectionToWorker(connectionIdRequestBody.getConnectionId());
  }

  public JobInfoRead resetConnectionStream(final ConnectionStreamRequestBody connectionStreamRequestBody)
      throws IOException {
    return submitResetConnectionStreamsToWorker(connectionStreamRequestBody.getConnectionId(), connectionStreamRequestBody.getStreams());
  }

  public JobInfoRead cancelJob(final JobIdRequestBody jobIdRequestBody) throws IOException {
    return submitCancellationToWorker(jobIdRequestBody.getId());
  }

  // Find all connections that use the source from the SourceDiscoverSchemaRequestBody. For each one,
  // determine whether 1. the source schema change resulted in a broken connection or 2. the user
  // wants the connection disabled when non-breaking changes are detected. If so, disable that
  // connection. Modify the current discoveredSchema object to add a CatalogDiff,
  // containsBreakingChange parameter, and connectionStatus parameter.
  private void generateCatalogDiffsAndDisableConnectionsIfNeeded(final SourceDiscoverSchemaRead discoveredSchema,
                                                                 final SourceDiscoverSchemaRequestBody discoverSchemaRequestBody,
                                                                 final UUID workspaceId)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final ConnectionReadList connectionsForSource = connectionsHandler.listConnectionsForSource(discoverSchemaRequestBody.getSourceId(), false);
    for (final ConnectionRead connectionRead : connectionsForSource.getConnections()) {
      final Optional<io.airbyte.api.model.generated.AirbyteCatalog> catalogUsedToMakeConfiguredCatalog = connectionsHandler
          .getConnectionAirbyteCatalog(connectionRead.getConnectionId());
      final io.airbyte.api.model.generated.@NotNull AirbyteCatalog currentAirbyteCatalog =
          connectionRead.getSyncCatalog();
      final CatalogDiff diff =
          connectionsHandler.getDiff(catalogUsedToMakeConfiguredCatalog.orElse(currentAirbyteCatalog), discoveredSchema.getCatalog(),
              CatalogConverter.toConfiguredProtocol(currentAirbyteCatalog));
      final boolean containsBreakingChange = containsBreakingChange(diff);

      if (containsBreakingChange) {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.BREAKING_SCHEMA_CHANGE_DETECTED, 1,
            new MetricAttribute(MetricTags.CONNECTION_ID, connectionRead.getConnectionId().toString()));
      } else {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NON_BREAKING_SCHEMA_CHANGE_DETECTED, 1,
            new MetricAttribute(MetricTags.CONNECTION_ID, connectionRead.getConnectionId().toString()));
      }

      final ConnectionUpdate updateObject =
          new ConnectionUpdate().breakingChange(containsBreakingChange).connectionId(connectionRead.getConnectionId());

      final ConnectionStatus connectionStatus;
      if (shouldDisableConnection(containsBreakingChange, connectionRead.getNonBreakingChangesPreference(), diff)) {
        connectionStatus = ConnectionStatus.INACTIVE;
      } else {
        connectionStatus = connectionRead.getStatus();
      }
      updateObject.status(connectionStatus);

      connectionsHandler.updateConnection(updateObject);

      if (shouldNotifySchemaChange(diff, connectionRead, discoverSchemaRequestBody)) {
        final String url = webUrlHelper.getConnectionUrl(workspaceId, connectionRead.getConnectionId());
        eventRunner.sendSchemaChangeNotification(connectionRead.getConnectionId(), url, containsBreakingChange);
      }
      if (connectionRead.getConnectionId().equals(discoverSchemaRequestBody.getConnectionId())) {
        discoveredSchema.catalogDiff(diff).breakingChange(containsBreakingChange).connectionStatus(connectionStatus);
      }
    }
  }

  private boolean shouldAutoPropagate(final CatalogDiff diff, final UUID workspaceId, final ConnectionRead connectionRead) {
    final boolean hasDiff = !diff.getTransforms().isEmpty();
    final boolean nonBreakingChange = !containsBreakingChange(diff);
    final boolean autoPropagationIsEnabledForWorkspace = featureFlagClient.boolVariation(AutoPropagateSchema.INSTANCE, new Workspace(workspaceId));
    final boolean autoPropagationIsEnabledForConnection =
        connectionRead.getNonBreakingChangesPreference() != null;
    return hasDiff && nonBreakingChange && autoPropagationIsEnabledForWorkspace && autoPropagationIsEnabledForConnection;
  }

  private void applySchemaChange(final UUID connectionId,
                                 final ConnectionUpdate updateObject,
                                 final io.airbyte.api.model.generated.AirbyteCatalog currentAirbyteCatalog,
                                 final io.airbyte.api.model.generated.AirbyteCatalog newCatalog,
                                 final List<StreamTransform> transformations,
                                 final UUID sourceCatalogId) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.SCHEMA_CHANGE_AUTO_PROPAGATED, 1,
        new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    final io.airbyte.api.model.generated.AirbyteCatalog catalog = getUpdatedSchema(
        currentAirbyteCatalog,
        newCatalog,
        transformations);
    updateObject.setSyncCatalog(catalog);
    updateObject.setSourceCatalogId(sourceCatalogId);
  }

  private boolean shouldNotifySchemaChange(final CatalogDiff diff,
                                           final ConnectionRead connectionRead,
                                           final SourceDiscoverSchemaRequestBody requestBody) {
    return !diff.getTransforms().isEmpty() && connectionRead.getNotifySchemaChanges() && requestBody.getNotifySchemaChange() != null
        && requestBody.getNotifySchemaChange();
  }

  private boolean shouldDisableConnection(final boolean containsBreakingChange,
                                          final NonBreakingChangesPreference preference,
                                          final CatalogDiff diff) {
    if (!envVariableFeatureFlags.autoDetectSchema()) {
      return false;
    }

    return containsBreakingChange || (preference == NonBreakingChangesPreference.DISABLE && !diff.getTransforms().isEmpty());
  }

  private CheckConnectionRead reportConnectionStatus(final SynchronousResponse<StandardCheckConnectionOutput> response) {
    final CheckConnectionRead checkConnectionRead = new CheckConnectionRead()
        .jobInfo(jobConverter.getSynchronousJobRead(response));

    if (response.isSuccess()) {
      checkConnectionRead
          .status(Enums.convertTo(response.getOutput().getStatus(), StatusEnum.class))
          .message(response.getOutput().getMessage());
    }

    return checkConnectionRead;
  }

  private JobInfoRead submitCancellationToWorker(final Long jobId) throws IOException {
    final Job job = jobPersistence.getJob(jobId);

    final ManualOperationResult cancellationResult = eventRunner.startNewCancellation(UUID.fromString(job.getScope()));
    if (cancellationResult.getFailingReason().isPresent()) {
      throw new IllegalStateException(cancellationResult.getFailingReason().get());
    }

    // query same job ID again to get updated job info after cancellation
    return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId));
  }

  private JobInfoRead submitManualSyncToWorker(final UUID connectionId)
      throws IOException, IllegalStateException, JsonValidationException, ConfigNotFoundException {
    // get standard sync to validate connection id before submitting sync to temporal
    final var sync = configRepository.getStandardSync(connectionId);
    if (!sync.getStatus().equals(StandardSync.Status.ACTIVE)) {
      throw new IllegalStateException("Can only sync an active connection");
    }
    final ManualOperationResult manualSyncResult = eventRunner.startNewManualSync(connectionId);

    return readJobFromResult(manualSyncResult);
  }

  private JobInfoRead submitResetConnectionToWorker(final UUID connectionId) throws IOException, IllegalStateException, ConfigNotFoundException {
    return submitResetConnectionToWorker(connectionId, configRepository.getAllStreamsForConnection(connectionId), false);
  }

  private JobInfoRead submitResetConnectionToWorker(final UUID connectionId,
                                                    final List<StreamDescriptor> streamsToReset,
                                                    final boolean runSyncImmediately)
      throws IOException {
    final ManualOperationResult resetConnectionResult = eventRunner.resetConnection(
        connectionId,
        streamsToReset,
        runSyncImmediately);

    return readJobFromResult(resetConnectionResult);
  }

  private JobInfoRead submitResetConnectionStreamsToWorker(final UUID connectionId, final List<ConnectionStream> streams)
      throws IOException, IllegalStateException {
    return submitResetConnectionToWorker(connectionId,
        streams.stream().map(s -> new StreamDescriptor().withName(s.getStreamName()).withNamespace(s.getStreamNamespace())).collect(
            Collectors.toList()),
        false);
  }

  private JobInfoRead readJobFromResult(final ManualOperationResult manualOperationResult) throws IOException, IllegalStateException {
    if (manualOperationResult.getFailingReason().isPresent()) {
      if (VALUE_CONFLICT_EXCEPTION_ERROR_CODE_SET.contains(manualOperationResult.getErrorCode().get())) {
        throw new ValueConflictKnownException(manualOperationResult.getFailingReason().get());
      } else {
        throw new IllegalStateException(manualOperationResult.getFailingReason().get());
      }
    }

    final Job job = jobPersistence.getJob(manualOperationResult.getJobId().get());

    return jobConverter.getJobInfoRead(job);
  }

  @VisibleForTesting
  boolean containsBreakingChange(final CatalogDiff diff) {
    for (final StreamTransform streamTransform : diff.getTransforms()) {
      if (streamTransform.getTransformType() != TransformTypeEnum.UPDATE_STREAM) {
        continue;
      }

      final boolean anyBreakingFieldTransforms = streamTransform.getUpdateStream().stream().anyMatch(FieldTransform::getBreaking);
      if (anyBreakingFieldTransforms) {
        return true;
      }
    }

    return false;
  }

}
