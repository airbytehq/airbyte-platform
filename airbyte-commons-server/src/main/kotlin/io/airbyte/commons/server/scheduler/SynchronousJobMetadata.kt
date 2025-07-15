/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import io.airbyte.commons.temporal.JobMetadata
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.JobConfig.ConfigType
import jakarta.annotation.Nullable
import java.nio.file.Path
import java.time.Instant
import java.util.Objects
import java.util.Optional
import java.util.UUID

/**
 * Job metadata for synchronous jobs. Provides common interface for this metadata to make handling
 * all synchronous requests easier.
 */
open class SynchronousJobMetadata(
  @JvmField val id: UUID,
  @JvmField val configType: ConfigType,
  private val configId: UUID?,
  @JvmField val createdAt: Long,
  @JvmField val endedAt: Long,
  val isSucceeded: Boolean,
  val isConnectorConfigurationUpdated: Boolean,
  @JvmField val logPath: Path?,
  @JvmField val failureReason: FailureReason?,
) {
  fun getConfigId(): Optional<UUID> = Optional.ofNullable(configId)

  override fun equals(o: Any?): Boolean {
    if (this === o) {
      return true
    }
    if (o == null || javaClass != o.javaClass) {
      return false
    }
    val that = o as SynchronousJobMetadata
    return createdAt == that.createdAt &&
      endedAt == that.endedAt &&
      isSucceeded == that.isSucceeded &&
      isConnectorConfigurationUpdated == that.isConnectorConfigurationUpdated &&
      id == that.id &&
      configType == that.configType &&
      configId == that.configId &&
      logPath == that.logPath &&
      failureReason == that.failureReason
  }

  override fun hashCode(): Int =
    Objects.hash(
      id,
      configType,
      configId,
      createdAt,
      endedAt,
      isSucceeded,
      isConnectorConfigurationUpdated,
      logPath,
      failureReason,
    )

  override fun toString(): String =
    (
      "SynchronousJobMetadata{" +
        "id=" + id +
        ", configType=" + configType +
        ", configId=" + configId +
        ", createdAt=" + createdAt +
        ", endedAt=" + endedAt +
        ", succeeded=" + isSucceeded +
        ", connectorConfigurationUpdated=" + isConnectorConfigurationUpdated +
        ", logPath=" + logPath +
        ", failureReason=" + failureReason +
        '}'
    )

  companion object {
    /**
     * Create synchronous job metadata from a temporal response.
     *
     * @param jobMetadata temporal job metadata
     * @param jobOutput output of job, if available
     * @param id job id
     * @param configType job type
     * @param configId id of resource for job type (i.e. if configType is discover config id is going to
     * be a source id)
     * @param createdAt time the job was created
     * @param endedAt time the job ended
     * @return synchronous job metadata
     */
    @JvmStatic
    fun fromJobMetadata(
      jobMetadata: JobMetadata,
      @Nullable jobOutput: ConnectorJobOutput?,
      id: UUID,
      configType: ConfigType,
      configId: UUID?,
      createdAt: Long,
      endedAt: Long,
    ): SynchronousJobMetadata =
      SynchronousJobMetadata(
        id,
        configType,
        configId,
        createdAt,
        endedAt,
        jobMetadata.succeeded,
        if (jobOutput != null) jobOutput.connectorConfigurationUpdated else false,
        jobMetadata.logPath,
        jobOutput?.failureReason,
      )

    /**
     * Create an empty object. This is used because some API interfaces assume that there will be this
     * metadata for jobs that don't produce it. This method is a convenience method to shim an empty
     * version of the metadata in those cases.
     *
     * @param configType config type
     * @return empty synchronous job metadata
     */
    @JvmStatic
    fun mock(configType: ConfigType): SynchronousJobMetadata {
      val now = Instant.now().toEpochMilli()
      val configId: UUID? = null
      val succeeded = true
      val connectorConfigurationUpdated = false
      val logPath: Path? = null
      val failureReason: FailureReason? = null

      return SynchronousJobMetadata(
        UUID.randomUUID(),
        configType,
        configId,
        now,
        now,
        succeeded,
        connectorConfigurationUpdated,
        logPath,
        failureReason,
      )
    }
  }
}
