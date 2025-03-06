/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator.config;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.EnvConfigs;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.pod.FileConstants;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Value;
import jakarta.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.nio.file.Path;

/**
 * Code for handling configuration files for orchestrated pods.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@Factory
public class ConfigFactory {

  /**
   * Returns the config directory which contains all the configuration files.
   *
   * @param configDir optional directory, defaults to FileConstants.CONFIG_DIR if not defined.
   * @return Configuration directory.
   */
  @Singleton
  @Named("configDir")
  String configDir(@Value("${airbyte.config-dir}") @Nullable final String configDir) {
    if (configDir == null) {
      return FileConstants.CONFIG_DIR;
    }
    return configDir;
  }

  /**
   * Returns the contents of the OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG file.
   *
   * @param jobId Which job is being run.
   * @param attemptId Which attempt of the job is being run.
   * @return Contents of OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG
   */
  @Singleton
  JobRunConfig jobRunConfig(@Value("${airbyte.job-id}") @Nullable final String jobId,
                            @Value("${airbyte.attempt-id}") @Nullable final long attemptId) {
    return new JobRunConfig().withJobId(jobId).withAttemptId(attemptId);
  }

  @Singleton
  @Named("workspaceRoot")
  public Path workspaceRoot(@Value("${airbyte.workspace-root}") final String workspaceRoot) {
    return Path.of(workspaceRoot);
  }

  @Singleton
  @Named("workloadId")
  public String workloadId(@Value("${airbyte.workload-id}") final String workloadId) {
    return workloadId;
  }

  @Singleton
  public ReplicationInput replicationInput(@Named("configDir") final String configDir) {
    return Jsons.deserialize(
        Path.of(configDir).resolve(FileConstants.INIT_INPUT_FILE).toFile(),
        ReplicationInput.class);
  }

  @Singleton
  EnvConfigs envConfigs() {
    return new EnvConfigs();
  }

}
