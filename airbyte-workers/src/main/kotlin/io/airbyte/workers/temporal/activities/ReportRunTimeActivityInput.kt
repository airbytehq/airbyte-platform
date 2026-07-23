/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.activities

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.util.UUID

@JsonDeserialize(builder = ReportRunTimeActivityInput.Builder::class)
data class ReportRunTimeActivityInput(
  val connectionId: UUID,
  val sourceDefinitionId: UUID,
  val startTime: Long,
  val refreshSchemaEndTime: Long,
  val replicationEndTime: Long,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionId: UUID? = null,
      private val sourceDefinitionId: UUID? = null,
      private val startTime: Long? = null,
      private val refreshSchemaEndTime: Long? = null,
      private val replicationEndTime: Long? = null,
    ) {
      fun build(): ReportRunTimeActivityInput =
        ReportRunTimeActivityInput(
          connectionId!!,
          sourceDefinitionId!!,
          startTime!!,
          refreshSchemaEndTime!!,
          replicationEndTime!!,
        )
    }
}
