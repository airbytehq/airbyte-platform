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
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.FailureReason;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.helpers.ResourceRequirementsUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.workers.general.DefaultCheckConnectionWorker;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.internal.VersionedAirbyteStreamFactory;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.process.AirbyteIntegrationLauncher;
import io.airbyte.workers.process.IntegrationLauncher;
import io.airbyte.workload.api.client.model.generated.WorkloadFailureRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadSuccessRequest;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

public class CheckJobOrchestrator implements JobOrchestrator<CheckConnectionInput> {

  private final CheckJobOrchestratorDataClass data;

  public CheckJobOrchestrator(final CheckJobOrchestratorDataClass data) {
    this.data = data;
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
    }
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

}
