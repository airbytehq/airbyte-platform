/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import static io.airbyte.workers.process.Metadata.ORCHESTRATOR_NORMALIZATION_STEP;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import java.util.Map;
import java.util.UUID;

/**
 * Normalization Launcher Worker.
 */
public class NormalizationLauncherWorker extends LauncherWorker<NormalizationInput, NormalizationSummary> {

  public static final String NORMALIZATION = "normalization-orchestrator";
  private static final String POD_NAME_PREFIX = "orchestrator-norm";
  public static final String INIT_FILE_DESTINATION_LAUNCHER_CONFIG = "destinationLauncherConfig.json";

  public NormalizationLauncherWorker(final UUID connectionId,
                                     final UUID workspaceId,
                                     final IntegrationLauncherConfig destinationLauncherConfig,
                                     final JobRunConfig jobRunConfig,
                                     final WorkerConfigs workerConfigs,
                                     final ContainerOrchestratorConfig containerOrchestratorConfig,
                                     final Integer serverPort,
                                     final FeatureFlagClient featureFlagClient,
                                     final MetricClient metricClient,
                                     final WorkloadIdGenerator workloadIdGenerator) {
    super(
        connectionId,
        workspaceId,
        NORMALIZATION,
        POD_NAME_PREFIX,
        jobRunConfig,
        Map.of(
            INIT_FILE_DESTINATION_LAUNCHER_CONFIG, Jsons.serialize(destinationLauncherConfig)),
        containerOrchestratorConfig,
        workerConfigs.getResourceRequirements(),
        NormalizationSummary.class,
        serverPort,
        workerConfigs,
        featureFlagClient,
        // Normalization process will happen only on a fixed set of connectors,
        // thus they are not going to be run under custom connectors. Setting this to false.
        false,
        metricClient,
        workloadIdGenerator);

  }

  @Override
  protected Map<String, String> generateCustomMetadataLabels() {
    return Map.of(SYNC_STEP_KEY, ORCHESTRATOR_NORMALIZATION_STEP);
  }

  @Override
  protected String getLauncherType() {
    return "Normalization";
  }

}
