/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.config

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.TemporalUtils
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.pod.FileConstants
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.annotation.Nullable
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.nio.file.Path
import java.util.UUID

/**
 * Code for handling configuration files for orchestrated pods.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@Factory
class ConfigFactory {
  @Singleton
  @Named("attemptId")
  fun attemptId(jobRunConfig: JobRunConfig) = Math.toIntExact(jobRunConfig.attemptId)

  @Singleton
  @Named("connectionId")
  fun connectionId(replicationInput: ReplicationInput): UUID = replicationInput.connectionId

  /**
   * Returns the config directory which contains all the configuration files.
   *
   * @param configDir optional directory, defaults to FileConstants.CONFIG_DIR if not defined.
   * @return Configuration directory.
   */
  @Singleton
  @Named("configDir")
  fun configDir(
    @Value("\${airbyte.config-dir}") @Nullable configDir: String?,
  ): String {
    if (configDir == null) {
      return FileConstants.CONFIG_DIR
    }
    return configDir
  }

  /**
   * Returns the contents of the OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG file.
   *
   * @param jobId Which job is being run.
   * @param attemptId Which attempt of the job is being run.
   * @return Contents of OrchestratorConstants.INIT_FILE_JOB_RUN_CONFIG
   */
  @Singleton
  fun jobRunConfig(
    @Value("\${airbyte.job-id}") @Nullable jobId: String,
    @Value("\${airbyte.attempt-id}") @Nullable attemptId: Long,
  ): JobRunConfig = JobRunConfig().withJobId(jobId).withAttemptId(attemptId)

  @Singleton
  @Named("jobRoot")
  fun jobRoot(
    jobRunConfig: JobRunConfig,
    @Named("workspaceRoot") workspaceRoot: Path,
  ): Path =
    TemporalUtils.getJobRoot(
      workspaceRoot,
      jobRunConfig.jobId,
      jobRunConfig.attemptId,
    )

  @Singleton
  @Named("workspaceRoot")
  fun workspaceRoot(
    @Value("\${airbyte.workspace-root}") workspaceRoot: String,
  ): Path = Path.of(workspaceRoot)

  @Singleton
  @Named("workloadId")
  fun workloadId(
    @Value("\${airbyte.workload-id}") workloadId: String,
  ): String = workloadId

  @Singleton
  fun replicationInput(
    @Named("configDir") configDir: String,
  ): ReplicationInput =
    Jsons.deserialize(
      Path.of(configDir).resolve(FileConstants.INIT_INPUT_FILE).toFile(),
      ReplicationInput::class.java,
    )

  @Singleton
  @Named("workspaceId")
  fun workspaceId(replicationInput: ReplicationInput): UUID = replicationInput.workspaceId
}
