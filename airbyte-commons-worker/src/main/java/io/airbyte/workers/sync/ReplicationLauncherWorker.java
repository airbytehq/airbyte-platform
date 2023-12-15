/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import static io.airbyte.workers.process.Metadata.ORCHESTRATOR_REPLICATION_STEP;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import java.util.Map;
import java.util.UUID;

/**
 * Launches a container-orchestrator container/pod to manage the message passing for the replication
 * step. This step configs onto the container-orchestrator and retrieves logs and the output from
 * the container-orchestrator.
 */
public class ReplicationLauncherWorker extends LauncherWorker<ReplicationInput, ReplicationOutput> {

  public static final String REPLICATION = "replication-orchestrator";
  public static final String POD_NAME_PREFIX = "orchestrator-repl";
  public static final String INIT_FILE_SOURCE_LAUNCHER_CONFIG = "sourceLauncherConfig.json";
  public static final String INIT_FILE_DESTINATION_LAUNCHER_CONFIG = "destinationLauncherConfig.json";

  public ReplicationLauncherWorker(final UUID connectionId,
                                   final UUID workspaceId,
                                   final ContainerOrchestratorConfig containerOrchestratorConfig,
                                   final IntegrationLauncherConfig sourceLauncherConfig,
                                   final IntegrationLauncherConfig destinationLauncherConfig,
                                   final JobRunConfig jobRunConfig,
                                   final ResourceRequirements resourceRequirements,
                                   final Integer serverPort,
                                   final WorkerConfigs workerConfigs,
                                   final FeatureFlagClient featureFlagClient,
                                   final MetricClient metricClient,
                                   final WorkloadIdGenerator workloadIdGenerator) {
    super(
        connectionId,
        workspaceId,
        REPLICATION,
        POD_NAME_PREFIX,
        jobRunConfig,
        Map.of(
            INIT_FILE_SOURCE_LAUNCHER_CONFIG, Jsons.serialize(sourceLauncherConfig),
            INIT_FILE_DESTINATION_LAUNCHER_CONFIG, Jsons.serialize(destinationLauncherConfig)),
        containerOrchestratorConfig,
        resourceRequirements,
        ReplicationOutput.class,
        serverPort,
        workerConfigs,
        featureFlagClient,
        sourceLauncherConfig.getIsCustomConnector() || destinationLauncherConfig.getIsCustomConnector(),
        metricClient,
        workloadIdGenerator);
  }

  @Override
  protected Map<String, String> generateCustomMetadataLabels() {
    return Map.of(SYNC_STEP_KEY, ORCHESTRATOR_REPLICATION_STEP);
  }

  @Override
  protected String getLauncherType() {
    return "Replication";
  }

}
