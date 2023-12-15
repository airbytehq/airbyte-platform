/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import static io.airbyte.workers.process.Metadata.ORCHESTRATOR_DBT_NORMALIZATION_STEP;
import static io.airbyte.workers.process.Metadata.SYNC_STEP_KEY;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.workers.config.WorkerConfigs;
import io.airbyte.config.OperatorDbtInput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.ContainerOrchestratorConfig;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import java.util.Map;
import java.util.UUID;

/**
 * Dbt Launcher Worker.
 */
public class DbtLauncherWorker extends LauncherWorker<OperatorDbtInput, Void> {

  public static final String DBT = "dbt-orchestrator";
  private static final String POD_NAME_PREFIX = "orchestrator-dbt";
  public static final String INIT_FILE_DESTINATION_LAUNCHER_CONFIG = "destinationLauncherConfig.json";

  public DbtLauncherWorker(final UUID connectionId,
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
        DBT,
        POD_NAME_PREFIX,
        jobRunConfig,
        Map.of(
            INIT_FILE_DESTINATION_LAUNCHER_CONFIG, Jsons.serialize(destinationLauncherConfig)),
        containerOrchestratorConfig,
        workerConfigs.getResourceRequirements(),
        Void.class,
        serverPort,
        workerConfigs,
        featureFlagClient,
        // Custom connector does not use Dbt at this moment, thus this flag for runnning job under
        // isolated pool can be set to false.
        false,
        metricClient,
        workloadIdGenerator);
  }

  @Override
  protected Map<String, String> generateCustomMetadataLabels() {
    return Map.of(SYNC_STEP_KEY, ORCHESTRATOR_DBT_NORMALIZATION_STEP);
  }

  @Override
  protected String getLauncherType() {
    return "DBT";
  }

}
