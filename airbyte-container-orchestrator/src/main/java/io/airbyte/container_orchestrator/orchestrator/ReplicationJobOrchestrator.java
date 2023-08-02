/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import static io.airbyte.metrics.lib.ApmTraceConstants.JOB_ORCHESTRATOR_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY;

import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.general.ReplicationWorker;
import io.airbyte.workers.general.ReplicationWorkerFactory;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs replication worker.
 */
public class ReplicationJobOrchestrator implements JobOrchestrator<StandardSyncInput> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Configs configs;
  private final JobRunConfig jobRunConfig;
  private final ReplicationWorkerFactory replicationWorkerFactory;
  // Used by the orchestrator to mark the job RUNNING once the relevant pods are spun up.
  private final AsyncStateManager asyncStateManager;

  public ReplicationJobOrchestrator(final Configs configs,
                                    final JobRunConfig jobRunConfig,
                                    final ReplicationWorkerFactory replicationWorkerFactory,
                                    final AsyncStateManager asyncStateManager) {
    this.configs = configs;
    this.jobRunConfig = jobRunConfig;
    this.replicationWorkerFactory = replicationWorkerFactory;
    this.asyncStateManager = asyncStateManager;
  }

  @Override
  public String getOrchestratorName() {
    return "Replication";
  }

  @Override
  public Class<StandardSyncInput> getInputClass() {
    return StandardSyncInput.class;
  }

  @Trace(operationName = JOB_ORCHESTRATOR_OPERATION_NAME)
  @Override
  public Optional<String> runJob() throws Exception {
    final var syncInput = readInput();

    final var sourceLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR, ReplicationLauncherWorker.INIT_FILE_SOURCE_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);

    final var destinationLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR, ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);
    log.info("sourceLauncherConfig is: " + sourceLauncherConfig.toString());

    ApmTraceUtils.addTagsToTrace(
        Map.of(JOB_ID_KEY, jobRunConfig.getJobId(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage()));

    final ReplicationWorker replicationWorker =
        replicationWorkerFactory.create(syncInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, this::markJobRunning);

    log.info("Running replication worker...");
    final var jobRoot = TemporalUtils.getJobRoot(configs.getWorkspaceRoot(),
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    final ReplicationOutput replicationOutput = replicationWorker.run(syncInput, jobRoot);

    log.info("Returning output...");
    return Optional.of(Jsons.serialize(replicationOutput));
  }

  private void markJobRunning() {
    asyncStateManager.write(AsyncKubePodStatus.RUNNING);
  }

}
