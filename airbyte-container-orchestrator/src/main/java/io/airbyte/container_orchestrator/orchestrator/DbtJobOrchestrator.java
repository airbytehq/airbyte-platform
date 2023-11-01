/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.orchestrator;

import static io.airbyte.metrics.lib.ApmTraceConstants.JOB_ORCHESTRATOR_OPERATION_NAME;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.commons.workers.config.WorkerConfigsProvider;
import io.airbyte.commons.workers.config.WorkerConfigsProvider.ResourceType;
import io.airbyte.config.Configs;
import io.airbyte.config.OperatorDbtInput;
import io.airbyte.container_orchestrator.AsyncStateManager;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.general.DbtTransformationRunner;
import io.airbyte.workers.general.DbtTransformationWorker;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubePodProcess;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.sync.ReplicationLauncherWorker;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the dbt normalization container.
 */
public class DbtJobOrchestrator implements JobOrchestrator<OperatorDbtInput> {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Configs configs;
  private final WorkerConfigsProvider workerConfigsProvider;
  private final ProcessFactory processFactory;
  private final JobRunConfig jobRunConfig;
  // Used by the orchestrator to mark the job RUNNING once the relevant pods are spun up.
  private final AsyncStateManager asyncStateManager;

  public DbtJobOrchestrator(final Configs configs,
                            final WorkerConfigsProvider workerConfigsProvider,
                            final ProcessFactory processFactory,
                            final JobRunConfig jobRunConfig,
                            final AsyncStateManager asyncStateManager) {
    this.configs = configs;
    this.workerConfigsProvider = workerConfigsProvider;
    this.processFactory = processFactory;
    this.jobRunConfig = jobRunConfig;
    this.asyncStateManager = asyncStateManager;
  }

  @Override
  public String getOrchestratorName() {
    return "DBT Transformation";
  }

  @Override
  public Class<OperatorDbtInput> getInputClass() {
    return OperatorDbtInput.class;
  }

  @Trace(operationName = JOB_ORCHESTRATOR_OPERATION_NAME)
  @Override
  public Optional<String> runJob() throws Exception {
    final OperatorDbtInput dbtInput = readInput();

    final IntegrationLauncherConfig destinationLauncherConfig = JobOrchestrator.readAndDeserializeFile(
        Path.of(KubePodProcess.CONFIG_DIR,
            ReplicationLauncherWorker.INIT_FILE_DESTINATION_LAUNCHER_CONFIG),
        IntegrationLauncherConfig.class);

    ApmTraceUtils
        .addTagsToTrace(Map.of(JOB_ID_KEY, jobRunConfig.getJobId(), DESTINATION_DOCKER_IMAGE_KEY,
            destinationLauncherConfig.getDockerImage()));

    log.info("Setting up dbt worker...");
    final WorkerConfigs workerConfigs = workerConfigsProvider.getConfig(ResourceType.DEFAULT);
    final DbtTransformationWorker worker = new DbtTransformationWorker(
        jobRunConfig.getJobId(),
        Math.toIntExact(jobRunConfig.getAttemptId()),
        workerConfigs.getResourceRequirements(),
        new DbtTransformationRunner(processFactory, destinationLauncherConfig.getDockerImage()),
        this::markJobRunning);

    log.info("Running dbt worker...");
    final Path jobRoot = TemporalUtils.getJobRoot(configs.getWorkspaceRoot(),
        jobRunConfig.getJobId(), jobRunConfig.getAttemptId());
    worker.run(dbtInput, jobRoot);

    return Optional.empty();
  }

  private void markJobRunning() {
    asyncStateManager.write(AsyncKubePodStatus.RUNNING);
  }

}
