/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import java.util.Optional

/**
 * Wraps the response from a temporal workflow. Adds metadata around whether it is a success or
 * error so that a caller can decide how to handle the output.
 *
 * @param <T> type of the output if it were a success.
</T> */
@JvmRecord
data class TemporalResponse<T : Any>(
  val output: T?,
  val metadata: JobMetadata,
) {
  fun isSuccess(): Boolean = metadata.succeeded

  fun getOutput(): Optional<T> = Optional.ofNullable(output)

  companion object {
    fun <T : Any> error(metadata: JobMetadata): TemporalResponse<T> = TemporalResponse(null, metadata)

    fun <T : Any> success(
      output: T,
      metadata: JobMetadata,
    ): TemporalResponse<T> = TemporalResponse(output, metadata)
  }
}
