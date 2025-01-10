/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import static io.airbyte.metrics.lib.MetricTags.CONNECTION_ID;
import static io.airbyte.metrics.lib.MetricTags.CONNECTOR_IMAGE;
import static io.airbyte.metrics.lib.MetricTags.CONNECTOR_TYPE;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ActorType;
import io.airbyte.api.client.model.generated.ConnectionAndJobIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.DestinationIdRequestBody;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.ResolveActorDefinitionVersionRequestBody;
import io.airbyte.api.client.model.generated.SaveStreamAttemptMetadataRequestBody;
import io.airbyte.api.client.model.generated.ScopeType;
import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.api.client.model.generated.StreamAttemptMetadata;
import io.airbyte.api.client.model.generated.SyncInput;
import io.airbyte.commons.converters.ApiClientConverters;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.helper.DockerImageName;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.SourceActorConfig;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.State;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.helpers.CatalogTransforms;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.helper.BackfillHelper;
import io.airbyte.workers.helper.MapperSecretHydrationHelper;
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper;
import io.airbyte.workers.input.ReplicationInputMapper;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import secrets.persistence.SecretCoordinateException;

public class ReplicationInputHydrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationInputHydrator.class);

  private final AirbyteApiClient airbyteApiClient;
  private final ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final MapperSecretHydrationHelper mapperSecretHydrationHelper;
  private final ReplicationInputMapper mapper;
  private final Boolean useRuntimeSecretPersistence;

  private final BackfillHelper backfillHelper;
  private final CatalogClientConverters catalogClientConverters;
  private final MetricClient metricClient;

  static final String FILE_TRANSFER_DELIVERY_TYPE = "use_file_transfer";

  public ReplicationInputHydrator(final AirbyteApiClient airbyteApiClient,
                                  final ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper,
                                  final SecretsRepositoryReader secretsRepositoryReader,
                                  final MapperSecretHydrationHelper mapperSecretHydrationHelper,
                                  final BackfillHelper backfillHelper,
                                  final CatalogClientConverters catalogClientConverters,
                                  final ReplicationInputMapper mapper,
                                  final MetricClient metricClient,
                                  final Boolean useRuntimeSecretPersistence) {
    this.airbyteApiClient = airbyteApiClient;
    this.backfillHelper = backfillHelper;
    this.catalogClientConverters = catalogClientConverters;
    this.resumableFullRefreshStatsHelper = resumableFullRefreshStatsHelper;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.mapperSecretHydrationHelper = mapperSecretHydrationHelper;
    this.mapper = mapper;
    this.metricClient = metricClient;
    this.useRuntimeSecretPersistence = useRuntimeSecretPersistence;
  }

  private <T> T retry(final CheckedSupplier<T> supplier) {
    return Failsafe.with(
        RetryPolicy.builder()
            .withBackoff(Duration.ofMillis(10), Duration.ofMillis(100))
            .withMaxRetries(5)
            .build())
        .get(supplier);
  }

  private void refreshSecretsReferences(final ReplicationActivityInput parsed) {
    final Object jobInput = retry(() -> airbyteApiClient.getJobsApi().getJobInput(
        new SyncInput(
            Long.parseLong(parsed.getJobRunConfig().getJobId()),
            parsed.getJobRunConfig().getAttemptId().intValue())));

    if (jobInput != null) {
      final JobInput apiResult = Jsons.convertValue(jobInput, JobInput.class);
      if (apiResult != null && apiResult.getSyncInput() != null) {
        final StandardSyncInput syncInput = apiResult.getSyncInput();

        if (syncInput.getSourceConfiguration() != null) {
          parsed.setSourceConfiguration(syncInput.getSourceConfiguration());
        }

        if (syncInput.getDestinationConfiguration() != null) {
          parsed.setDestinationConfiguration(syncInput.getDestinationConfiguration());
        }
      }
    }
  }

  /**
   * Converts a ReplicationActivityInput -- passed through Temporal to the replication activity -- to
   * a ReplicationInput which will be passed down the stack to the actual
   * source/destination/orchestrator processes.
   *
   * @param replicationActivityInput the input passed from the sync workflow to the replication
   *        activity
   * @return the input to be passed down to the source/destination/orchestrator processes
   * @throws Exception from the Airbyte API
   */
  public ReplicationInput getHydratedReplicationInput(final ReplicationActivityInput replicationActivityInput) throws Exception {
    ApmTraceUtils.addTagsToTrace(Map.of("api_base_url", airbyteApiClient.getDestinationApi().getBaseUrl()));
    refreshSecretsReferences(replicationActivityInput);
    final var destination =
        airbyteApiClient.getDestinationApi().getDestination(new DestinationIdRequestBody(replicationActivityInput.getDestinationId()));
    final var tag = DockerImageName.INSTANCE.extractTag(replicationActivityInput.getDestinationLauncherConfig().getDockerImage());
    final var resolvedDestinationVersion = airbyteApiClient.getActorDefinitionVersionApi().resolveActorDefinitionVersionByTag(
        new ResolveActorDefinitionVersionRequestBody(destination.getDestinationDefinitionId(), ActorType.DESTINATION, tag));

    final SourceActorConfig sourceActorConfig = Jsons.object(replicationActivityInput.getSourceConfiguration(), SourceActorConfig.class);
    final boolean useFileTransfer = sourceActorConfig.getUseFileTransfer() || (sourceActorConfig.getDeliveryMethod() != null
        && FILE_TRANSFER_DELIVERY_TYPE.equals(sourceActorConfig.getDeliveryMethod().getDeliveryType()));

    if (useFileTransfer && !resolvedDestinationVersion.getSupportFileTransfer()) {
      final String errorMessage = "Destination does not support file transfers, but source requires it. The destination version is: "
          + resolvedDestinationVersion.getDockerImageTag();
      LOGGER.error(errorMessage);
      throw new WorkerException(errorMessage);
    }

    // Retrieve the connection, which we need in a few places.
    final long jobId = Long.parseLong(replicationActivityInput.getJobRunConfig().getJobId());
    final ConnectionRead connectionInfo = resolvedDestinationVersion.getSupportRefreshes()
        ? airbyteApiClient.getConnectionApi()
            .getConnectionForJob(new ConnectionAndJobIdRequestBody(replicationActivityInput.getConnectionId(), jobId))
        : airbyteApiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody(replicationActivityInput.getConnectionId()));

    final ConfiguredAirbyteCatalog catalog = retrieveCatalog(connectionInfo);
    if (replicationActivityInput.isReset()) {
      // If this is a reset, we need to set the streams being reset to Full Refresh | Overwrite.
      updateCatalogForReset(replicationActivityInput, catalog);
    }
    // Retrieve the state.
    State state = retrieveState(replicationActivityInput);
    List<StreamDescriptor> streamsToBackfill = null;
    if (backfillHelper.syncShouldBackfill(replicationActivityInput, connectionInfo)) {
      streamsToBackfill = backfillHelper.getStreamsToBackfill(replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff(), catalog);
      state =
          getUpdatedStateForBackfill(state, replicationActivityInput.getSchemaRefreshOutput(), replicationActivityInput.getConnectionId(), catalog);
    }

    try {
      trackBackfillAndResume(
          jobId,
          replicationActivityInput.getJobRunConfig().getAttemptId(),
          resumableFullRefreshStatsHelper.getStreamsWithStates(state).stream().toList(),
          streamsToBackfill);
    } catch (final Exception e) {
      LOGGER.error("Failed to track stream metadata for connectionId:{} attempt:{}", replicationActivityInput.getConnectionId(),
          replicationActivityInput.getJobRunConfig().getAttemptId(), e);
    }

    // Hydrate the secrets.
    final JsonNode fullDestinationConfig;
    final JsonNode fullSourceConfig;
    final UUID organizationId = replicationActivityInput.getConnectionContext().getOrganizationId();

    // If the organization is configured to use "run time secrets management" aka "bring your own
    // secrets manager", then we must look up their secrets config and hydrate from there.
    // TODO: The runtime secrets client and the default secrets client should implement the same
    // interface, so we can avoid this conditional look up and delegation in the hydrator itself and do
    // it at the injection layer.
    if (useRuntimeSecretPersistence && organizationId != null) {
      try {
        final RuntimeSecretPersistence runtimeSecretPersistence = getRuntimeSecretPersistence(organizationId);
        fullSourceConfig = secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(replicationActivityInput.getSourceConfiguration(),
            runtimeSecretPersistence);
        fullDestinationConfig =
            secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(replicationActivityInput.getDestinationConfiguration(),
                runtimeSecretPersistence);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    } else { // use default configured persistence
      try {
        fullSourceConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationActivityInput.getSourceConfiguration());
      } catch (final SecretCoordinateException e) {
        metricClient.count(
            OssMetricsRegistry.SECRETS_HYDRATION_FAILURE, 1,
            new MetricAttribute(CONNECTOR_IMAGE, replicationActivityInput.getSourceLauncherConfig().getDockerImage()),
            new MetricAttribute(CONNECTOR_TYPE, ActorType.SOURCE.toString()),
            new MetricAttribute(CONNECTION_ID, replicationActivityInput.getSourceLauncherConfig().getConnectionId().toString()));
        throw e;
      }
      try {
        fullDestinationConfig =
            secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationActivityInput.getDestinationConfiguration());
      } catch (final SecretCoordinateException e) {
        metricClient.count(
            OssMetricsRegistry.SECRETS_HYDRATION_FAILURE, 1,
            new MetricAttribute(CONNECTOR_IMAGE, replicationActivityInput.getDestinationLauncherConfig().getDockerImage()),
            new MetricAttribute(CONNECTOR_TYPE, ActorType.DESTINATION.toString()),
            new MetricAttribute(CONNECTION_ID, replicationActivityInput.getDestinationLauncherConfig().getConnectionId().toString()));
        throw e;
      }
    }

    // Hydrate mapper secrets
    final ConfiguredAirbyteCatalog hydratedCatalog =
        mapperSecretHydrationHelper.hydrateMapperSecrets(catalog, useRuntimeSecretPersistence, organizationId);

    return mapper.toReplicationInput(replicationActivityInput)
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig)
        .withCatalog(hydratedCatalog)
        .withState(state)
        .withDestinationSupportsRefreshes(resolvedDestinationVersion.getSupportRefreshes());
  }

  private RuntimeSecretPersistence getRuntimeSecretPersistence(final UUID organizationId) throws IOException {
    final SecretPersistenceConfig secretPersistenceConfig = airbyteApiClient.getSecretPersistenceConfigApi().getSecretsPersistenceConfig(
        new SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, organizationId));
    return new RuntimeSecretPersistence(
        fromApiSecretPersistenceConfig(secretPersistenceConfig));
  }

  @VisibleForTesting
  void trackBackfillAndResume(final Long jobId,
                              final Long attemptNumber,
                              final List<StreamDescriptor> streamsWithStates,
                              final List<StreamDescriptor> streamsToBackfill)
      throws IOException {
    final Map<StreamDescriptor, StreamAttemptMetadata> metadataPerStream = streamsWithStates != null ? streamsWithStates
        .stream()
        .map(s -> Map.entry(s, new StreamAttemptMetadata(s.getName(), false, true, s.getNamespace())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)) : new HashMap<>();

    if (streamsToBackfill != null) {
      for (final StreamDescriptor stream : streamsToBackfill) {
        final StreamAttemptMetadata attemptMetadata = metadataPerStream.get(stream);
        if (attemptMetadata == null) {
          metadataPerStream.put(stream, new StreamAttemptMetadata(stream.getName(), true, false, stream.getNamespace()));
        } else {
          metadataPerStream.put(stream, new StreamAttemptMetadata(stream.getName(), true, true, stream.getNamespace()));
        }
      }
    }

    airbyteApiClient.getAttemptApi()
        .saveStreamMetadata(new SaveStreamAttemptMetadataRequestBody(jobId, attemptNumber.intValue(), metadataPerStream.values().stream().toList()));
  }

  private State getUpdatedStateForBackfill(final State state,
                                           final RefreshSchemaActivityOutput schemaRefreshOutput,
                                           final UUID connectionId,
                                           final ConfiguredAirbyteCatalog catalog)
      throws Exception {
    if (schemaRefreshOutput != null && schemaRefreshOutput.getAppliedDiff() != null) {
      final var streamsToBackfill = backfillHelper.getStreamsToBackfill(schemaRefreshOutput.getAppliedDiff(), catalog);
      LOGGER.debug("Backfilling streams: {}", String.join(", ", streamsToBackfill.stream().map(StreamDescriptor::getName).toList()));
      final State resetState = backfillHelper.clearStateForStreamsToBackfill(state, streamsToBackfill);
      if (resetState != null) {
        // We persist the state here in case the attempt fails, the subsequent attempt will continue the
        // backfill process.
        // TODO(mfsiega-airbyte): move all of the state handling into a separate activity.
        LOGGER.debug("Resetting state for connection: {}", connectionId);
        persistState(resetState, connectionId);
      }

      return resetState;
    }
    return state;
  }

  @NotNull
  private ConfiguredAirbyteCatalog retrieveCatalog(final ConnectionRead connectionInfo) {
    if (connectionInfo.getSyncCatalog() == null) {
      throw new IllegalArgumentException("Connection is missing catalog, which is required");
    }
    final ConfiguredAirbyteCatalog catalog =
        catalogClientConverters.toConfiguredAirbyteInternal(connectionInfo.getSyncCatalog());
    return catalog;
  }

  private void persistState(final State resetState, final UUID connectionId) throws IOException {
    final StateWrapper stateWrapper = StateMessageHelper.getTypedState(resetState.getState()).get();
    final ConnectionState connectionState = StateConverter.toClient(connectionId, stateWrapper);

    airbyteApiClient.getStateApi().createOrUpdateState(new ConnectionStateCreateOrUpdate(connectionId, connectionState));
  }

  private State retrieveState(final ReplicationActivityInput replicationActivityInput) throws IOException {
    final ConnectionState connectionState =
        airbyteApiClient.getStateApi().getState(new ConnectionIdRequestBody(replicationActivityInput.getConnectionId()));
    final State state =
        connectionState != null && !ConnectionStateType.NOT_SET.equals(connectionState.getStateType())
            ? StateMessageHelper.getState(StateConverter.toInternal(StateConverter.fromClientToApi(connectionState)))
            : null;
    return state;
  }

  private void updateCatalogForReset(final ReplicationActivityInput replicationActivityInput, final ConfiguredAirbyteCatalog catalog)
      throws IOException {
    final JobOptionalRead jobInfo = airbyteApiClient.getJobsApi().getLastReplicationJob(
        new ConnectionIdRequestBody(replicationActivityInput.getConnectionId()));
    final boolean hasStreamsToReset = jobInfo != null && jobInfo.getJob() != null && jobInfo.getJob().getResetConfig() != null
        && jobInfo.getJob().getResetConfig().getStreamsToReset() != null;
    if (hasStreamsToReset) {
      final var streamsToReset =
          jobInfo.getJob().getResetConfig().getStreamsToReset().stream().map(ApiClientConverters::toInternal).toList();
      CatalogTransforms.updateCatalogForReset(streamsToReset, catalog);
    }
  }

  private io.airbyte.config.SecretPersistenceConfig fromApiSecretPersistenceConfig(final SecretPersistenceConfig apiSecretPersistenceConfig) {
    return new io.airbyte.config.SecretPersistenceConfig()
        .withScopeType(Enums.convertTo(apiSecretPersistenceConfig.getScopeType(), io.airbyte.config.ScopeType.class))
        .withScopeId(apiSecretPersistenceConfig.getScopeId())
        .withConfiguration(Jsons.deserializeToStringMap(apiSecretPersistenceConfig.getConfiguration()))
        .withSecretPersistenceType(
            Enums.convertTo(
                apiSecretPersistenceConfig.getSecretPersistenceType(),
                io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType.class));
  }

}
