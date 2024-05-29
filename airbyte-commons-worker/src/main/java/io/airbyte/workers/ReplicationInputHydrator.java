/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionAndJobIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionRead;
import io.airbyte.api.client.model.generated.ConnectionState;
import io.airbyte.api.client.model.generated.ConnectionStateCreateOrUpdate;
import io.airbyte.api.client.model.generated.ConnectionStateType;
import io.airbyte.api.client.model.generated.JobOptionalRead;
import io.airbyte.api.client.model.generated.ScopeType;
import io.airbyte.api.client.model.generated.SecretPersistenceConfig;
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody;
import io.airbyte.api.client.model.generated.StreamDescriptor;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.converters.ProtocolConverters;
import io.airbyte.commons.converters.StateConverter;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.protocol.CatalogTransforms;
import io.airbyte.config.State;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.helpers.StateMessageHelper;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.featureflag.ActivateRefreshes;
import io.airbyte.featureflag.AutoBackfillOnNewColumns;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.workers.helper.BackfillHelper;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationInputHydrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationInputHydrator.class);

  private final AirbyteApiClient airbyteApiClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final FeatureFlagClient featureFlagClient;

  public ReplicationInputHydrator(final AirbyteApiClient airbyteApiClient,
                                  final SecretsRepositoryReader secretsRepositoryReader,
                                  final FeatureFlagClient featureFlagClient) {
    this.airbyteApiClient = airbyteApiClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.featureFlagClient = featureFlagClient;
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

    final boolean canRunRefreshes = featureFlagClient.boolVariation(ActivateRefreshes.INSTANCE, new Multi(
        List.of(
            new Workspace(replicationActivityInput.getWorkspaceId()),
            new Connection(replicationActivityInput.getConnectionId()))));
    // Retrieve the connection, which we need in a few places.
    final ConnectionRead connectionInfo = canRunRefreshes
        ? airbyteApiClient.getConnectionApi().getConnectionForJob(new ConnectionAndJobIdRequestBody(replicationActivityInput.getConnectionId(),
            Long.parseLong(replicationActivityInput.getJobRunConfig().getJobId())))
        : airbyteApiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody(replicationActivityInput.getConnectionId()));

    final ConfiguredAirbyteCatalog catalog = retrieveCatalog(connectionInfo);
    if (replicationActivityInput.getIsReset()) {
      // If this is a reset, we need to set the streams being reset to Full Refresh | Overwrite.
      updateCatalogForReset(replicationActivityInput, catalog);
    }
    // Retrieve the state.
    State state = retrieveState(replicationActivityInput);
    final boolean backfillEnabledForWorkspace =
        featureFlagClient.boolVariation(AutoBackfillOnNewColumns.INSTANCE, new Workspace(replicationActivityInput.getWorkspaceId()));
    if (backfillEnabledForWorkspace && BackfillHelper.syncShouldBackfill(replicationActivityInput, connectionInfo)) {
      state =
          getUpdatedStateForBackfill(state, replicationActivityInput.getSchemaRefreshOutput(), replicationActivityInput.getConnectionId(), catalog);
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
    return new ReplicationInput()
        .withNamespaceDefinition(replicationActivityInput.getNamespaceDefinition())
        .withNamespaceFormat(replicationActivityInput.getNamespaceFormat())
        .withPrefix(replicationActivityInput.getPrefix())
        .withSourceId(replicationActivityInput.getSourceId())
        .withDestinationId(replicationActivityInput.getDestinationId())
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig)
        .withSyncResourceRequirements(replicationActivityInput.getSyncResourceRequirements())
        .withWorkspaceId(replicationActivityInput.getWorkspaceId())
        .withConnectionId(replicationActivityInput.getConnectionId())
        .withNormalizeInDestinationContainer(replicationActivityInput.getNormalizeInDestinationContainer())
        .withIsReset(replicationActivityInput.getIsReset())
        .withJobRunConfig(replicationActivityInput.getJobRunConfig())
        .withSourceLauncherConfig(replicationActivityInput.getSourceLauncherConfig())
        .withDestinationLauncherConfig(replicationActivityInput.getDestinationLauncherConfig())
        .withCatalog(catalog)
        .withState(state);
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
    final ConfiguredAirbyteCatalog catalog = CatalogClientConverters.toConfiguredAirbyteProtocol(connectionInfo.getSyncCatalog());
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
          jobInfo.getJob().getResetConfig().getStreamsToReset().stream().map(ProtocolConverters::clientStreamDescriptorToProtocol).toList();
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
