/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.activities

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.util.UUID

@JsonDeserialize(builder = SyncFeatureFlagFetcherInput.Builder::class)
data class SyncFeatureFlagFetcherInput(
  val connectionId: UUID,
  val sourceDefinitionId: UUID,
  val workspaceId: UUID,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionId: UUID? = null,
      private val sourceDefinitionId: UUID? = null,
      val workspaceId: UUID? = null,
    ) {
      fun build(): SyncFeatureFlagFetcherInput =
        SyncFeatureFlagFetcherInput(
          connectionId!!,
          sourceDefinitionId!!,
          workspaceId!!,
        )
    }
}
