/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import static io.airbyte.metrics.lib.ApmTraceConstants.JOB_ORCHESTRATOR_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.general.DefaultNormalizationWorker;
import io.airbyte.workers.normalization.DefaultNormalizationRunner;
import io.airbyte.workers.normalization.NormalizationWorker;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Run normalization worker.
 */
@Slf4j
public class NormalizationJobOrchestrator implements JobOrchestrator<NormalizationInput> {

  private final Configs configs;
  private final ProcessFactory processFactory;
  private final JobRunConfig jobRunConfig;
  // Used by the orchestrator to mark the job RUNNING once the relevant pods are spun up.
  private final AsyncStateManager asyncStateManager;

  public NormalizationJobOrchestrator(final Configs configs,
                                      final ProcessFactory processFactory,
                                      final JobRunConfig jobRunConfig,
                                      final AsyncStateManager asyncStateManager) {
    this.configs = configs;
    this.processFactory = processFactory;
    this.jobRunConfig = jobRunConfig;
    this.asyncStateManager = asyncStateManager;
  }

  @Override
  public String getOrchestratorName() {
    return "Normalization";
  }

  @Override
  public Class<NormalizationInput> getInputClass() {
    return NormalizationInput.class;
  }

  @Trace(operationName = JOB_ORCHESTRATOR_OPERATION_NAME)
  @Override
  public Optional<String> runJob() throws Exception {
    // final JobRunConfig jobRunConfig = readJobRunConfig();
    final NormalizationInput normalizationInput = readInput();

    final IntegrationLauncherConfig destinationLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR,
            ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);

    ApmTraceUtils
        .addTagsToTrace(Map.of(JOB_ID_KEY, jobRunConfig.getJobId(), DESTINATION_DOCKER_IMAGE_KEY,
            destinationLauncherConfig.getDockerImage()));

    log.info("Setting up normalization worker...");
    final NormalizationWorker normalizationWorker = new DefaultNormalizationWorker(
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        new DefaultNormalizationRunner(
            processFactory,
            destinationLauncherConfig.getNormalizationDockerImage(),
            destinationLauncherConfig.getNormalizationIntegrationType()),
        configs.getWorkerEnvironment(),
        this::markJobRunning);

    log.info("Running normalization worker...");
    final Path jobRoot = TemporalUtils.getJobRoot(configs.getWorkspaceRoot(),
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    final NormalizationSummary normalizationSummary = normalizationWorker.run(normalizationInput,
        jobRoot);

    return Optional.of(Jsons.serialize(normalizationSummary));
  }

  private void markJobRunning() {
    asyncStateManager.write(AsyncKubePodStatus.RUNNING);
  }

}
