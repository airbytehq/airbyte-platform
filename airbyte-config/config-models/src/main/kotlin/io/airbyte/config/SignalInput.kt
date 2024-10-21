package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import java.io.Serializable

@JsonDeserialize(builder = SignalInput.Builder::class)
data class SignalInput(
  @JsonProperty("workflow_type")
  val workflowType: String,
  @JsonProperty("workflow_id")
  val workflowId: String,
) : Serializable {
  companion object {
    const val SYNC_WORKFLOW = "sync"
  }

  data class Builder
    @JvmOverloads
    constructor(
      @JsonProperty("workflow_type")
      var workflowType: String? = null,
      @JsonProperty("workflow_id")
      var workflowId: String? = null,
    ) {
      fun workflowType(type: String) = apply { this.workflowType = type }

      fun workflowId(workflowId: String) = apply { this.workflowId = workflowId }

      fun build(): SignalInput =
        SignalInput(
          workflowType = workflowType ?: throw IllegalArgumentException("workflowType cannot be null"),
          workflowId = workflowId ?: throw IllegalArgumentException("workflowId cannot be null"),
        )
    }
}
