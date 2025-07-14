/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.scheduler

import io.airbyte.commons.temporal.JobMetadata
import io.airbyte.commons.temporal.TemporalResponse
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.JobConfig.ConfigType
import java.util.Objects
import java.util.UUID
import java.util.function.Function

/**
 * Response wrapper for synchronous temporal jobs.
 *
 * @param <T> output type of the job
</T> */
open class SynchronousResponse<T>(
  @JvmField val output: T,
  @JvmField val metadata: SynchronousJobMetadata,
) {
  val isSuccess: Boolean
    get() = metadata.isSucceeded

  override fun equals(o: Any?): Boolean {
    if (this === o) {
      return true
    }
    if (o == null || javaClass != o.javaClass) {
      return false
    }
    val that = o as SynchronousResponse<*>
    return output == that.output && metadata == that.metadata
  }

  override fun hashCode(): Int = Objects.hash(output, metadata)

  override fun toString(): String =
    (
      "SynchronousResponse{" +
        "output=" + output +
        ", metadata=" + metadata +
        '}'
    )

  companion object {
    fun <T> error(metadata: SynchronousJobMetadata): SynchronousResponse<T?> = SynchronousResponse(null, metadata)

    fun <T> success(
      output: T,
      metadata: SynchronousJobMetadata,
    ): SynchronousResponse<T> = SynchronousResponse(output, metadata)

    /**
     * Response from synchronous temporal job.
     *
     * @param temporalResponse response from temporal client
     * @param outputMapper function to retrieve output of the job
     * @param id job id
     * @param configType job type
     * @param configId id of resource for job type (i.e. if configType is discover config id is going to
     * be a source id)
     * @param createdAt time the job was created
     * @param endedAt time the job ended
     * @param <T> output for job type
     * @param <U> output type for job type of temporal job
     * @return response
     </U></T> */
    fun <T> fromTemporalResponse(
      temporalResponse: TemporalResponse<ConnectorJobOutput>,
      outputMapper: Function<ConnectorJobOutput, T>,
      id: UUID,
      configType: ConfigType,
      configId: UUID?,
      createdAt: Long,
      endedAt: Long,
    ): SynchronousResponse<T> {
      val jobOutput = temporalResponse.getOutput()
      val responseOutput = jobOutput.map(outputMapper).orElse(null)

      val metadataResponse =
        if (responseOutput == null) {
          JobMetadata(false, temporalResponse.metadata.logPath)
        } else {
          temporalResponse.metadata
        }

      val metadata =
        SynchronousJobMetadata.fromJobMetadata(
          metadataResponse,
          jobOutput.orElse(null),
          id,
          configType,
          configId,
          createdAt,
          endedAt,
        )
      return SynchronousResponse(responseOutput, metadata)
    }
  }
}
