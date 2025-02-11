/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.activities

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.ConnectionContext
import java.time.Duration
import java.util.UUID

@JsonDeserialize(builder = GetConnectionContextInput.Builder::class)
data class GetConnectionContextInput(
  val connectionId: UUID,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionId: UUID? = null,
    ) {
      fun build(): GetConnectionContextInput =
        GetConnectionContextInput(
          connectionId!!,
        )
    }
}

@JsonDeserialize(builder = GetConnectionContextOutput.Builder::class)
data class GetConnectionContextOutput(
  val connectionContext: ConnectionContext,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionContext: ConnectionContext? = null,
    ) {
      fun build(): GetConnectionContextOutput =
        GetConnectionContextOutput(
          connectionContext!!,
        )
    }
}

@JsonDeserialize(builder = GetLoadShedBackoffInput.Builder::class)
data class GetLoadShedBackoffInput(
  val connectionContext: ConnectionContext,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionContext: ConnectionContext? = null,
    ) {
      fun build(): GetLoadShedBackoffInput =
        GetLoadShedBackoffInput(
          connectionContext!!,
        )
    }
}

@JsonDeserialize(builder = GetLoadShedBackoffOutput.Builder::class)
data class GetLoadShedBackoffOutput(
  val duration: Duration,
) {
  class Builder
    @JvmOverloads
    constructor(
      val duration: Duration? = null,
    ) {
      fun build(): GetLoadShedBackoffOutput =
        GetLoadShedBackoffOutput(
          duration!!,
        )
    }
}
