/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.version.AirbyteProtocolVersion;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.WorkloadHeartbeatRate;
import io.airbyte.featureflag.WorkloadHeartbeatTimeout;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.workers.general.DefaultCheckConnectionWorker;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadHeartbeatRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckJobOrchestrator implements JobOrchestrator<CheckConnectionInput> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CheckJobOrchestrator.class);
  private final CheckJobOrchestratorDataClass data;
  private final ExecutorService heartbeatExecutorService;

  public CheckJobOrchestrator(final CheckJobOrchestratorDataClass data) {
    this.data = data;
    this.heartbeatExecutorService = Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "check-job-orchestrator-heartbeat");
      thread.setDaemon(true);
      return thread;
    });
  }

  @Override
  public String getOrchestratorName() {
    return "Check connection";
  }

  @Override
  public Class<CheckConnectionInput> getInputClass() {
    return CheckConnectionInput.class;
  }

  @Override
  public Optional<String> runJob() throws Exception {
    final CheckConnectionInput input = readInput();

    // TODO: This is assuming that the input is already hydrated and doesnt need to go through
    // inputHydrator, get this verified.
    // Compare this with CheckConnectionActivityImpl
    final StandardCheckConnectionInput connectionConfiguration = input.getConnectionConfiguration();
    final String workloadId =
        data.workloadIdGenerator().generateCheckWorkloadId(connectionConfiguration.getActorId(), input.getJobRunConfig().getJobId(),
            Math.toIntExact(input.getJobRunConfig().getAttemptId()));
    final Path jobRoot = TemporalUtils.getJobRoot(data.configs().getWorkspaceRoot(), workloadId);

    try {
      final Context featureFlagContext = getFeatureFlagContext(connectionConfiguration.getActorContext());
      startHeartbeat(workloadId,
          Duration.ofSeconds(data.featureFlagClient().intVariation(WorkloadHeartbeatRate.INSTANCE, featureFlagContext)),
          Duration.ofMinutes(data.featureFlagClient().intVariation(WorkloadHeartbeatTimeout.INSTANCE, featureFlagContext)));
      final ConnectorJobOutput output =
          worker(input.getLauncherConfig(), connectionConfiguration.getResourceRequirements()).run(connectionConfiguration,
              jobRoot);
      data.jobOutputDocStore().write(workloadId, output);
      succeedWorkload(workloadId);
      return Optional.of(Jsons.serialize(output));
    } catch (final Exception e) {
      ConnectorJobOutput output = new ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
          .withCheckConnection(new StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED)
              .withMessage("The check connection failed."))
          .withFailureReason(new FailureReason()
              .withFailureOrigin(connectionConfiguration.getActorType() == ActorType.SOURCE ? FailureReason.FailureOrigin.SOURCE
                  : FailureReason.FailureOrigin.DESTINATION)
              .withExternalMessage("The check connection failed because of an internal error")
              .withInternalMessage(e.getMessage())
              .withStacktrace(e.toString()));
      data.jobOutputDocStore().write(workloadId, output);
      failWorkload(workloadId, output.getFailureReason());
      return Optional.of(Jsons.serialize(output));
    } finally {
      heartbeatExecutorService.shutdownNow();
    }
  }

  private void startHeartbeat(final String workloadId, final Duration heartbeatInterval, final Duration heartbeatTimeoutDuration) {
    heartbeatExecutorService.execute(() -> {
      Instant lastSuccessfulHeartbeat = Instant.now();
      do {
        try {
          Thread.sleep(heartbeatInterval.toMillis());
          data.workloadApi().workloadHeartbeat(new WorkloadHeartbeatRequest(workloadId));
          lastSuccessfulHeartbeat = Instant.now();
        } catch (final Exception e) {
          if (e instanceof ClientException && ((ClientException) e).getStatusCode() == HttpStatus.GONE.getCode()) {
            LOGGER.warn("Received kill response from API, shutting down heartbeat", e);
            // Unlike ReplicationOrchestrator we are not manually cancelling the worker process, just wait for
            // it to die/complete by itself
            break;
          } else if (Duration.between(lastSuccessfulHeartbeat, Instant.now()).compareTo(heartbeatTimeoutDuration) > 0) {
            LOGGER.warn("Have not been able to update heartbeat for more than the timeout duration, shutting down heartbeat", e);
            // Unlike ReplicationOrchestrator we are not manually cancelling the worker process, just wait for
            // it to die/complete by itself
            break;
          }
          LOGGER.warn("Error while trying to heartbeat, re-trying", e);
        }
      } while (true);
    });
  }

  private DefaultCheckConnectionWorker worker(final IntegrationLauncherConfig launcherConfig,
                                              final ResourceRequirements actorDefinitionResourceRequirements) {
    final WorkerConfigs workerConfigs = data.workerConfigsProvider().getConfig(WorkerConfigsProvider.ResourceType.CHECK);
    final ResourceRequirements defaultWorkerConfigResourceRequirements = workerConfigs.getResourceRequirements();

    final IntegrationLauncher integrationLauncher = new AirbyteIntegrationLauncher(
        launcherConfig.getJobId(),
        Math.toIntExact(launcherConfig.getAttemptId()),
        launcherConfig.getConnectionId(),
        launcherConfig.getWorkspaceId(),
        launcherConfig.getDockerImage(),
        data.processFactory(),
        ResourceRequirementsUtils.getResourceRequirements(actorDefinitionResourceRequirements, defaultWorkerConfigResourceRequirements),
        null,
        launcherConfig.getAllowedHosts(),
        launcherConfig.getIsCustomConnector(),
        data.featureFlags(),
        launcherConfig.getAdditionalEnvironmentVariables(),
        launcherConfig.getAdditionalLabels());

    final ConnectorConfigUpdater connectorConfigUpdater = new ConnectorConfigUpdater(
        data.airbyteApiClient().getSourceApi(),
        data.airbyteApiClient().getDestinationApi());

    final var protocolVersion =
        launcherConfig.getProtocolVersion() != null ? launcherConfig.getProtocolVersion() : AirbyteProtocolVersion.DEFAULT_AIRBYTE_PROTOCOL_VERSION;
    final AirbyteStreamFactory streamFactory =
        new VersionedAirbyteStreamFactory<>(data.serDeProvider(), data.migratorFactory(), protocolVersion, Optional.empty(), Optional.empty(),
            new VersionedAirbyteStreamFactory.InvalidLineFailureConfiguration(false, false, false),
            data.gsonPksExtractor());

    return new DefaultCheckConnectionWorker(integrationLauncher, connectorConfigUpdater, streamFactory);

  }

  private void failWorkload(final String workloadId, final FailureReason failureReason) throws IOException {
    if (failureReason != null) {
      data.workloadApi().workloadFailure(new WorkloadFailureRequest(workloadId, failureReason.getFailureOrigin().value(),
          failureReason.getExternalMessage()));
    } else {
      data.workloadApi().workloadFailure(new WorkloadFailureRequest(workloadId, null, null));
    }
  }

  private void succeedWorkload(final String workloadId) throws IOException {
    data.workloadApi().workloadSuccess(new WorkloadSuccessRequest(workloadId));
  }

  private static Context getFeatureFlagContext(final ActorContext actorContext) {
    final List<Context> contexts = new ArrayList<>();
    if (actorContext.getWorkspaceId() != null) {
      contexts.add(new Workspace(actorContext.getWorkspaceId()));
    }
    if (actorContext.getOrganizationId() != null) {
      contexts.add(new Organization(actorContext.getOrganizationId()));
    }
    if (actorContext.getActorType() != null) {
      switch (actorContext.getActorType()) {
        case SOURCE -> {
          if (actorContext.getActorId() != null) {
            contexts.add(new Source(actorContext.getActorId()));
          }
          if (actorContext.getActorDefinitionId() != null) {
            contexts.add(new SourceDefinition(actorContext.getActorDefinitionId()));
          }
        }
        case DESTINATION -> {
          if (actorContext.getActorId() != null) {
            contexts.add(new Destination(actorContext.getActorId()));
          }
          if (actorContext.getActorDefinitionId() != null) {
            contexts.add(new DestinationDefinition(actorContext.getActorDefinitionId()));
          }
        }
        default -> throw new IllegalArgumentException("Unknown actor type " + actorContext.getActorType().toString());
      }
    }
    return new Multi(contexts);
  }

}
