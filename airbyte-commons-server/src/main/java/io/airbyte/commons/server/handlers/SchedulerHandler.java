/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.getUpdatedSchema;
import static io.airbyte.config.helpers.ResourceRequirementsUtils.getResourceRequirementsForJobType;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
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
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationSyncMode;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.JobConfigType;
import io.airbyte.api.model.generated.JobCreate;
import io.airbyte.api.model.generated.JobIdRequestBody;
import io.airbyte.api.model.generated.JobInfoRead;
import io.airbyte.api.model.generated.LogRead;
import io.airbyte.api.model.generated.NonBreakingChangesPreference;
import io.airbyte.api.model.generated.SourceAutoPropagateChange;
import io.airbyte.api.model.generated.SourceCoreConfig;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead;
import io.airbyte.api.model.generated.SourceDiscoverSchemaRequestBody;
import io.airbyte.api.model.generated.SourceIdRequestBody;
import io.airbyte.api.model.generated.SourceUpdate;
import io.airbyte.api.model.generated.StreamTransform;
import io.airbyte.api.model.generated.SynchronousJobRead;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.features.FeatureFlags;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper;
import io.airbyte.commons.server.handlers.helpers.AutoPropagateSchemaChangeHelper.UpdateSchemaResult;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.server.scheduler.EventRunner;
import io.airbyte.commons.server.scheduler.SynchronousResponse;
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient;
import io.airbyte.commons.temporal.ErrorCode;
import io.airbyte.commons.temporal.TemporalClient.ManualOperationResult;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorCatalog;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobTypeResourceLimit.JobType;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionWorkspaces.UseNewSchemaUpdateNotification;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.notification.CustomerioNotificationClient;
import io.airbyte.notification.SlackNotificationClient;
import io.airbyte.persistence.job.JobCreator;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.WebUrlHelper;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.factory.SyncJobFactory;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.tracker.JobTracker;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ScheduleHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("ParameterName")
@Singleton
@Slf4j
public class SchedulerHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerHandler.class);
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
  private final StreamResetPersistence streamResetPersistence;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final JobCreator jobCreator;
  private final SyncJobFactory jobFactory;
  private final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper;
  private final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler;
  private final WorkspaceService workspaceService;
  private final SecretPersistenceConfigService secretPersistenceConfigService;

  @VisibleForTesting
  public SchedulerHandler(final ConfigRepository configRepository,
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
                          final FeatureFlagClient featureFlagClient,
                          final StreamResetPersistence streamResetPersistence,
                          final OAuthConfigSupplier oAuthConfigSupplier,
                          final JobCreator jobCreator,
                          final SyncJobFactory jobFactory,
                          final JobNotifier jobNotifier,
                          final JobTracker jobTracker,
                          final ConnectorDefinitionSpecificationHandler connectorDefinitionSpecificationHandler,
                          final WorkspaceService workspaceService,
                          final SecretPersistenceConfigService secretPersistenceConfigService) {
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
    this.streamResetPersistence = streamResetPersistence;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.jobCreator = jobCreator;
    this.jobFactory = jobFactory;
    this.connectorDefinitionSpecificationHandler = connectorDefinitionSpecificationHandler;
    this.workspaceService = workspaceService;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.jobCreationAndStatusUpdateHelper = new JobCreationAndStatusUpdateHelper(
        jobPersistence,
        configRepository,
        jobNotifier,
        jobTracker);
  }

  public CheckConnectionRead checkSourceConnectionFromSourceId(final SourceIdRequestBody sourceIdRequestBody)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID sourceId = sourceIdRequestBody.getSourceId();
    final SourceConnection source = configRepository.getSourceConnection(sourceId);
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(source.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.getWorkspaceId(), sourceId);
    final boolean isCustomConnector = sourceDef.getCustom();
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        ResourceRequirementsUtils.getResourceRequirementsForJobType(sourceDef.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);

    return reportConnectionStatus(
        synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, isCustomConnector, resourceRequirements));
  }

  public CheckConnectionRead checkSourceConnectionFromSourceCreate(final SourceCoreConfig sourceConfig)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(sourceConfig.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConfig.getWorkspaceId(), sourceConfig.getSourceId());
    // split out secrets
    final JsonNode partialConfig = sanitizePartialConfig(
        sourceConfig.getWorkspaceId(),
        sourceConfig.getConnectionConfiguration(),
        sourceVersion.getSpec());

    // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
    // technically declared as required.
    final SourceConnection source = new SourceConnection()
        .withSourceId(sourceConfig.getSourceId())
        .withSourceDefinitionId(sourceConfig.getSourceDefinitionId())
        .withConfiguration(partialConfig)
        .withWorkspaceId(sourceConfig.getWorkspaceId());

    final boolean isCustomConnector = sourceDef.getCustom();
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        ResourceRequirementsUtils.getResourceRequirementsForJobType(sourceDef.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);

    return reportConnectionStatus(
        synchronousSchedulerClient.createSourceCheckConnectionJob(source, sourceVersion, isCustomConnector, resourceRequirements));
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
    final boolean isCustomConnector = destinationDef.getCustom();
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        getResourceRequirementsForJobType(destinationDef.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);
    return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, isCustomConnector, resourceRequirements));
  }

  public CheckConnectionRead checkDestinationConnectionFromDestinationCreate(final DestinationCoreConfig destinationConfig)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardDestinationDefinition destDef = configRepository.getStandardDestinationDefinition(destinationConfig.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destDef, destinationConfig.getWorkspaceId(), destinationConfig.getDestinationId());
    final var partialConfig = sanitizePartialConfig(
        destinationConfig.getWorkspaceId(),
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
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        getResourceRequirementsForJobType(destDef.getResourceRequirements(), JobType.CHECK_CONNECTION).orElse(null);

    return reportConnectionStatus(
        synchronousSchedulerClient.createDestinationCheckConnectionJob(destination, destinationVersion, isCustomConnector, resourceRequirements));
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
    final boolean isCustomConnector = sourceDef.getCustom();
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.getResourceRequirements(), JobType.DISCOVER_SCHEMA).orElse(null);

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
              sourceVersion,
              isCustomConnector,
              resourceRequirements);
      final SourceDiscoverSchemaRead discoveredSchema = retrieveDiscoveredSchema(persistedCatalogId, sourceVersion);

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
        .catalog(CatalogConverter.toApi(airbyteCatalog, sourceVersion))
        .jobInfo(emptyJob)
        .catalogId(currentCatalog.get().getId());
  }

  public void applySchemaChangeForSource(final SourceAutoPropagateChange sourceAutoPropagateChange)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    LOGGER.info("Applying schema changes for source '{}' in workspace '{}'",
        sourceAutoPropagateChange.getSourceId(), sourceAutoPropagateChange.getWorkspaceId());
    if (sourceAutoPropagateChange.getSourceId() == null) {
      LOGGER.warn("Missing required field sourceId for applying schema change.");
      return;
    }

    if (sourceAutoPropagateChange.getWorkspaceId() == null
        || sourceAutoPropagateChange.getCatalogId() == null
        || sourceAutoPropagateChange.getCatalog() == null) {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.MISSING_APPLY_SCHEMA_CHANGE_INPUT, 1,
          new MetricAttribute(MetricTags.SOURCE_ID, sourceAutoPropagateChange.getSourceId().toString()));
      LOGGER.warn("Missing required fields for applying schema change. sourceId: {}, workspaceId: {}, catalogId: {}, catalog: {}",
          sourceAutoPropagateChange.getSourceId(), sourceAutoPropagateChange.getWorkspaceId(), sourceAutoPropagateChange.getCatalogId(),
          sourceAutoPropagateChange.getCatalog());
      return;
    }

    final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(sourceAutoPropagateChange.getWorkspaceId(), true);
    final SourceConnection source = configRepository.getSourceConnection(sourceAutoPropagateChange.getSourceId());
    final NotificationSettings notificationSettings = workspace.getNotificationSettings();
    final ConnectionReadList connectionsForSource =
        connectionsHandler.listConnectionsForSource(sourceAutoPropagateChange.getSourceId(), false);
    for (final ConnectionRead connectionRead : connectionsForSource.getConnections()) {
      final Optional<io.airbyte.api.model.generated.AirbyteCatalog> catalogUsedToMakeConfiguredCatalog = connectionsHandler
          .getConnectionAirbyteCatalog(connectionRead.getConnectionId());
      final io.airbyte.api.model.generated.@NotNull AirbyteCatalog syncCatalog =
          connectionRead.getSyncCatalog();
      final CatalogDiff diff =
          connectionsHandler.getDiff(catalogUsedToMakeConfiguredCatalog.orElse(syncCatalog),
              sourceAutoPropagateChange.getCatalog(),
              CatalogConverter.toConfiguredProtocol(syncCatalog));

      final ConnectionUpdate updateObject =
          new ConnectionUpdate().connectionId(connectionRead.getConnectionId());
      final UUID destinationDefinitionId =
          configRepository.getDestinationDefinitionFromConnection(connectionRead.getConnectionId()).getDestinationDefinitionId();
      final var supportedDestinationSyncModes =
          connectorDefinitionSpecificationHandler
              .getDestinationSpecification(new DestinationDefinitionIdWithWorkspaceId().destinationDefinitionId(destinationDefinitionId)
                  .workspaceId(sourceAutoPropagateChange.getWorkspaceId()))
              .getSupportedDestinationSyncModes();

      if (AutoPropagateSchemaChangeHelper.shouldAutoPropagate(diff, connectionRead)) {
        final UpdateSchemaResult result = applySchemaChange(updateObject.getConnectionId(),
            sourceAutoPropagateChange.getWorkspaceId(),
            updateObject,
            syncCatalog,
            sourceAutoPropagateChange.getCatalog(),
            diff.getTransforms(),
            sourceAutoPropagateChange.getCatalogId(),
            connectionRead.getNonBreakingChangesPreference(), supportedDestinationSyncModes);
        connectionsHandler.updateConnection(updateObject);
        connectionsHandler.trackSchemaChange(sourceAutoPropagateChange.getWorkspaceId(),
            updateObject.getConnectionId(), result);
        LOGGER.info("Propagating changes for connectionId: '{}', new catalogId '{}'",
            connectionRead.getConnectionId(), sourceAutoPropagateChange.getCatalogId());
        final boolean newNotificationsEnabled = featureFlagClient.boolVariation(
            UseNewSchemaUpdateNotification.INSTANCE, new Workspace(sourceAutoPropagateChange.getWorkspaceId()));
        if (notificationSettings != null && newNotificationsEnabled && notificationSettings.getSendOnConnectionUpdate() != null) {
          notifySchemaPropagated(notificationSettings, diff, workspace, connectionRead, source,
              workspace.getEmail(), result);
        }
      } else {
        LOGGER.info("Not propagating changes for connectionId: '{}', new catalogId '{}'",
            connectionRead.getConnectionId(), sourceAutoPropagateChange.getCatalogId());
      }
    }
  }

  public void notifySchemaPropagated(final NotificationSettings notificationSettings,
                                     final CatalogDiff diff,
                                     final StandardWorkspace workspace,
                                     final ConnectionRead connection,
                                     final SourceConnection source,
                                     final String email,
                                     final UpdateSchemaResult result)
      throws IOException {
    final NotificationItem item;
    final String connectionUrl = webUrlHelper.getConnectionUrl(workspace.getWorkspaceId(), connection.getConnectionId());
    final boolean isBreakingChange = AutoPropagateSchemaChangeHelper.containsBreakingChange(diff);
    if (isBreakingChange) {
      item = notificationSettings.getSendOnConnectionUpdateActionRequired();
    } else {
      item = notificationSettings.getSendOnConnectionUpdate();
    }
    for (final NotificationType type : item.getNotificationType()) {
      try {
        switch (type) {
          case SLACK -> {
            final SlackNotificationClient slackNotificationClient = new SlackNotificationClient(item.getSlackConfiguration());
            slackNotificationClient.notifySchemaPropagated(
                workspace.getWorkspaceId(),
                workspace.getName(),
                connection.getConnectionId(),
                connection.getName(),
                connectionUrl,
                source.getSourceId(),
                source.getName(),
                result.changeDescription(),
                email,
                isBreakingChange);
          }
          case CUSTOMERIO -> {
            final CustomerioNotificationClient emailNotificationClient = new CustomerioNotificationClient();
            emailNotificationClient.notifySchemaPropagated(
                workspace.getWorkspaceId(),
                workspace.getName(),
                connection.getConnectionId(),
                connection.getName(),
                connectionUrl,
                source.getSourceId(),
                source.getName(),
                result.changeDescription(),
                email,
                isBreakingChange);
          }
          default -> {
            LOGGER.warn("Notification type {} not supported", type);
          }
        }
      } catch (final InterruptedException e) {
        LOGGER.error("Failed to send notification for connectionId: '{}'", connection.getConnectionId(), e);
      }
    }
  }

  public SourceDiscoverSchemaRead discoverSchemaForSourceFromSourceCreate(final SourceCoreConfig sourceCreate)
      throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardSourceDefinition sourceDef = configRepository.getStandardSourceDefinition(sourceCreate.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceCreate.getWorkspaceId(), sourceCreate.getSourceId());
    final var partialConfig = sanitizePartialConfig(
        sourceCreate.getWorkspaceId(),
        sourceCreate.getConnectionConfiguration(),
        sourceVersion.getSpec());

    final boolean isCustomConnector = sourceDef.getCustom();
    // ResourceRequirements are read from actor definition and can be null; but if it's not null it will
    // have higher priority and overwrite
    // the default settings in WorkerConfig.
    final ResourceRequirements resourceRequirements =
        getResourceRequirementsForJobType(sourceDef.getResourceRequirements(), JobType.DISCOVER_SCHEMA).orElse(null);
    // todo (cgardens) - narrow the struct passed to the client. we are not setting fields that are
    // technically declared as required.
    final SourceConnection source = new SourceConnection()
        .withSourceDefinitionId(sourceCreate.getSourceDefinitionId())
        .withConfiguration(partialConfig)
        .withWorkspaceId(sourceCreate.getWorkspaceId());
    final SynchronousResponse<UUID> response = synchronousSchedulerClient.createDiscoverSchemaJob(
        source,
        sourceVersion,
        isCustomConnector,
        resourceRequirements);
    return retrieveDiscoveredSchema(response, sourceVersion);
  }

  private SourceDiscoverSchemaRead retrieveDiscoveredSchema(final SynchronousResponse<UUID> response, final ActorDefinitionVersion sourceVersion)
      throws ConfigNotFoundException, IOException {
    final SourceDiscoverSchemaRead sourceDiscoverSchemaRead = new SourceDiscoverSchemaRead()
        .jobInfo(jobConverter.getSynchronousJobRead(response));

    if (response.isSuccess()) {
      final ActorCatalog catalog = configRepository.getActorCatalogById(response.getOutput());
      final AirbyteCatalog persistenceCatalog = Jsons.object(catalog.getCatalog(),
          io.airbyte.protocol.models.AirbyteCatalog.class);
      sourceDiscoverSchemaRead.catalog(CatalogConverter.toApi(persistenceCatalog, sourceVersion));
      sourceDiscoverSchemaRead.catalogId(response.getOutput());
    }

    return sourceDiscoverSchemaRead;
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

  public JobInfoRead createJob(final JobCreate jobCreate) throws JsonValidationException, ConfigNotFoundException, IOException {
    // Fail non-terminal jobs first to prevent failing to create a new job
    jobCreationAndStatusUpdateHelper.failNonTerminalJobs(jobCreate.getConnectionId());

    final StandardSync standardSync = configRepository.getStandardSync(jobCreate.getConnectionId());
    final List<StreamDescriptor> streamsToReset = streamResetPersistence.getStreamResets(jobCreate.getConnectionId());
    log.info("Found the following streams to reset for connection {}: {}", jobCreate.getConnectionId(), streamsToReset);

    if (!streamsToReset.isEmpty()) {
      final DestinationConnection destination = configRepository.getDestinationConnection(standardSync.getDestinationId());

      final JsonNode destinationConfiguration = oAuthConfigSupplier.injectDestinationOAuthParameters(
          destination.getDestinationDefinitionId(),
          destination.getDestinationId(),
          destination.getWorkspaceId(),
          destination.getConfiguration());
      destination.setConfiguration(destinationConfiguration);

      final StandardDestinationDefinition destinationDef =
          configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
      final ActorDefinitionVersion destinationVersion =
          actorDefinitionVersionHelper.getDestinationVersion(destinationDef, destination.getWorkspaceId(), destination.getDestinationId());
      final String destinationImageName = destinationVersion.getDockerRepository() + ":" + destinationVersion.getDockerImageTag();

      final List<StandardSyncOperation> standardSyncOperations = Lists.newArrayList();
      for (final var operationId : standardSync.getOperationIds()) {
        final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationId);
        // NOTE: we must run normalization operations during resets, because we rely on them to clear the
        // normalized tables. However, we don't want to run other operations (dbt, webhook) because those
        // are meant to transform the data after the sync but there's no data to transform. Webhook
        // operations particularly will fail because we don't populate some required config during resets.
        if (StandardSyncOperation.OperatorType.NORMALIZATION.equals(standardSyncOperation.getOperatorType())) {
          standardSyncOperations.add(standardSyncOperation);
        }
      }

      final Optional<Long> jobIdOptional =
          jobCreator.createResetConnectionJob(
              destination,
              standardSync,
              destinationDef,
              destinationVersion,
              destinationImageName,
              new Version(destinationVersion.getProtocolVersion()),
              destinationDef.getCustom(),
              standardSyncOperations,
              streamsToReset,
              destination.getWorkspaceId());

      final long jobId = jobIdOptional.isEmpty()
          ? jobPersistence.getLastReplicationJob(standardSync.getConnectionId()).orElseThrow(() -> new RuntimeException("No job available")).getId()
          : jobIdOptional.get();

      return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId));
    } else {
      final long jobId = jobFactory.create(jobCreate.getConnectionId());

      log.info("New job created, with id: " + jobId);
      final Job job = jobPersistence.getJob(jobId);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CREATED_BY_RELEASE_STAGE, job);

      return jobConverter.getJobInfoRead(jobPersistence.getJob(jobId));
    }
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
      final boolean containsBreakingChange = AutoPropagateSchemaChangeHelper.containsBreakingChange(diff);

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
      // on workspace where the new detailed schema change notification is enabled, notifications are sent
      // right after the diff is computed, and we
      // don't need to send them here.
      final boolean newNotificationsEnabled = featureFlagClient.boolVariation(UseNewSchemaUpdateNotification.INSTANCE, new Workspace(workspaceId));
      if (shouldNotifySchemaChange(diff, connectionRead, discoverSchemaRequestBody) && !newNotificationsEnabled) {
        final String url = webUrlHelper.getConnectionReplicationPageUrl(workspaceId, connectionRead.getConnectionId());
        final String sourceName = configRepository.getSourceConnection(connectionRead.getSourceId()).getName();
        eventRunner.sendSchemaChangeNotification(connectionRead.getConnectionId(), connectionRead.getName(), sourceName, url, containsBreakingChange);
      }
      if (connectionRead.getConnectionId().equals(discoverSchemaRequestBody.getConnectionId())) {
        discoveredSchema.catalogDiff(diff).breakingChange(containsBreakingChange).connectionStatus(connectionStatus);
      }
    }
  }

  private UpdateSchemaResult applySchemaChange(final UUID connectionId,
                                               final UUID workspaceId,
                                               final ConnectionUpdate updateObject,
                                               final io.airbyte.api.model.generated.AirbyteCatalog currentSyncCatalog,
                                               final io.airbyte.api.model.generated.AirbyteCatalog newCatalog,
                                               final List<StreamTransform> transformations,
                                               final UUID sourceCatalogId,
                                               final NonBreakingChangesPreference nonBreakingChangesPreference,
                                               final List<DestinationSyncMode> supportedDestinationSyncModes) {
    MetricClientFactory.getMetricClient().count(OssMetricsRegistry.SCHEMA_CHANGE_AUTO_PROPAGATED, 1,
        new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
    final UpdateSchemaResult updateSchemaResult = getUpdatedSchema(
        currentSyncCatalog,
        newCatalog,
        transformations,
        nonBreakingChangesPreference,
        supportedDestinationSyncModes,
        featureFlagClient, workspaceId);
    updateObject.setSyncCatalog(updateSchemaResult.catalog());
    updateObject.setSourceCatalogId(sourceCatalogId);
    return updateSchemaResult;
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

  /**
   * Wrapper around
   * {@link SecretsRepositoryWriter#statefulSplitSecretsToDefaultSecretPersistence(JsonNode, ConnectorSpecification)}.
   *
   * @param workspaceId workspaceId
   * @param connectionConfiguration connectionConfiguration
   * @param connectorSpecification connector specification
   * @return config with secrets replaced with secret json
   */
  @SuppressWarnings("PMD.PreserveStackTrace")
  private JsonNode sanitizePartialConfig(final UUID workspaceId,
                                         final JsonNode connectionConfiguration,
                                         final ConnectorSpecification connectorSpecification)
      throws IOException, ConfigNotFoundException {
    // split out secrets
    final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      final SecretPersistenceConfig secretPersistenceConfig;
      try {
        secretPersistenceConfig = secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get());
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
      return secretsRepositoryWriter.statefulSplitSecretsToRuntimeSecretPersistence(
          connectionConfiguration,
          connectorSpecification,
          new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      return secretsRepositoryWriter.statefulSplitSecretsToDefaultSecretPersistence(
          connectionConfiguration,
          connectorSpecification);
    }
  }

}
