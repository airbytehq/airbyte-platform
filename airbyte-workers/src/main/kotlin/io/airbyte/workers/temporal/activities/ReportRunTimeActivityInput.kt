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
  val shouldRefreshSchema: Boolean,
) {
  class Builder
    @JvmOverloads
    constructor(
      val connectionId: UUID? = null,
      val sourceDefinitionId: UUID? = null,
      val startTime: Long? = null,
      val refreshSchemaEndTime: Long? = null,
      val replicationEndTime: Long? = null,
      val shouldRefreshSchema: Boolean? = null,
    ) {
      fun build(): ReportRunTimeActivityInput {
        return ReportRunTimeActivityInput(
          connectionId!!,
          sourceDefinitionId!!,
          startTime!!,
          refreshSchemaEndTime!!,
          replicationEndTime!!,
          shouldRefreshSchema!!,
        )
      }
    }
}
