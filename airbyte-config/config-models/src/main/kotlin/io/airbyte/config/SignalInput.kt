package io.airbyte.config

data class SignalInput(
  val workflowType: String,
  val workflowId: String,
  val taskQueue: String,
) {
  companion object {
    const val SYNC_WORKFLOW = "sync"
  }
}
