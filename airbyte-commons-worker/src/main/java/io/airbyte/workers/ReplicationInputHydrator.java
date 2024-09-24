/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

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
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.State;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.helpers.CatalogTransforms;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.EnableMappers;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.helper.BackfillHelper;
import io.airbyte.workers.helper.ResumableFullRefreshStatsHelper;
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

public class ReplicationInputHydrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationInputHydrator.class);

  private final AirbyteApiClient airbyteApiClient;
  private final ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final FeatureFlagClient featureFlagClient;

  public ReplicationInputHydrator(final AirbyteApiClient airbyteApiClient,
                                  final ResumableFullRefreshStatsHelper resumableFullRefreshStatsHelper,
                                  final SecretsRepositoryReader secretsRepositoryReader,
                                  final FeatureFlagClient featureFlagClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.resumableFullRefreshStatsHelper = resumableFullRefreshStatsHelper;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.featureFlagClient = featureFlagClient;
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

    // Retrieve the connection, which we need in a few places.
    final long jobId = Long.parseLong(replicationActivityInput.getJobRunConfig().getJobId());
    final ConnectionRead connectionInfo = resolvedDestinationVersion.getSupportRefreshes()
        ? airbyteApiClient.getConnectionApi()
            .getConnectionForJob(new ConnectionAndJobIdRequestBody(replicationActivityInput.getConnectionId(), jobId))
        : airbyteApiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody(replicationActivityInput.getConnectionId()));

    final ConfiguredAirbyteCatalog catalog = retrieveCatalog(connectionInfo);
    if (replicationActivityInput.getIsReset()) {
      // If this is a reset, we need to set the streams being reset to Full Refresh | Overwrite.
      updateCatalogForReset(replicationActivityInput, catalog);
    }
    // Retrieve the state.
    State state = retrieveState(replicationActivityInput);
    List<StreamDescriptor> streamsToBackfill = null;
    final boolean shouldEnableMappers = featureFlagClient.boolVariation(EnableMappers.INSTANCE, new Connection(connectionInfo.getConnectionId()));
    if (BackfillHelper.syncShouldBackfill(replicationActivityInput, connectionInfo, shouldEnableMappers)) {
      streamsToBackfill = BackfillHelper.getStreamsToBackfill(replicationActivityInput.getSchemaRefreshOutput().getAppliedDiff(), catalog);
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
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig = airbyteApiClient.getSecretPersistenceConfigApi().getSecretsPersistenceConfig(
            new SecretPersistenceConfigGetRequestBody(ScopeType.ORGANIZATION, organizationId));
        final RuntimeSecretPersistence runtimeSecretPersistence = new RuntimeSecretPersistence(
            fromApiSecretPersistenceConfig(secretPersistenceConfig));
        fullSourceConfig = secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(replicationActivityInput.getSourceConfiguration(),
            runtimeSecretPersistence);
        fullDestinationConfig =
            secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(replicationActivityInput.getDestinationConfiguration(),
                runtimeSecretPersistence);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      fullSourceConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationActivityInput.getSourceConfiguration());
      fullDestinationConfig =
          secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(replicationActivityInput.getDestinationConfiguration());
    }
    return mapActivityInputToReplInput(replicationActivityInput)
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig)
        .withCatalog(catalog)
        .withState(state)
        .withDestinationSupportsRefreshes(resolvedDestinationVersion.getSupportRefreshes());
  }

  /**
   * Converts ReplicationActivityInput to ReplicationInput by mapping basic files. Does NOT perform
   * any hydration. Does not copy unhydrated config.
   */
  public ReplicationInput mapActivityInputToReplInput(final ReplicationActivityInput replicationActivityInput) {
    return new ReplicationInput()
        .withNamespaceDefinition(replicationActivityInput.getNamespaceDefinition())
        .withNamespaceFormat(replicationActivityInput.getNamespaceFormat())
        .withPrefix(replicationActivityInput.getPrefix())
        .withSourceId(replicationActivityInput.getSourceId())
        .withDestinationId(replicationActivityInput.getDestinationId())
        .withSyncResourceRequirements(replicationActivityInput.getSyncResourceRequirements())
        .withWorkspaceId(replicationActivityInput.getWorkspaceId())
        .withConnectionId(replicationActivityInput.getConnectionId())
        .withIsReset(replicationActivityInput.getIsReset())
        .withJobRunConfig(replicationActivityInput.getJobRunConfig())
        .withSourceLauncherConfig(replicationActivityInput.getSourceLauncherConfig())
        .withDestinationLauncherConfig(replicationActivityInput.getDestinationLauncherConfig());
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
      final var streamsToBackfill = BackfillHelper.getStreamsToBackfill(schemaRefreshOutput.getAppliedDiff(), catalog);
      LOGGER.debug("Backfilling streams: {}", String.join(", ", streamsToBackfill.stream().map(StreamDescriptor::getName).toList()));
      final State resetState = BackfillHelper.clearStateForStreamsToBackfill(state, streamsToBackfill);
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
    final boolean shouldEnableMappers = featureFlagClient.boolVariation(EnableMappers.INSTANCE, new Connection(connectionInfo.getConnectionId()));
    final ConfiguredAirbyteCatalog catalog =
        CatalogClientConverters.toConfiguredAirbyteInternal(connectionInfo.getSyncCatalog(), shouldEnableMappers);
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
