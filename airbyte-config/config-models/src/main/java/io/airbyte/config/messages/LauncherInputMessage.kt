package io.airbyte.config.messages

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = LauncherInputMessage.Builder::class)
data class LauncherInputMessage(
  val workloadId: String,
  val workloadInput: String,
  val labels: Map<String, String>,
  val logPath: String,
) {
  data class Builder(
    var workloadId: String? = null,
    var workloadInput: String? = null,
    var labels: Map<String, String>? = null,
    var logPath: String? = null,
  ) {
    fun workloadId(workloadId: String) = apply { this.workloadId = workloadId }

    fun workloadInput(workloadInput: String) = apply { this.workloadInput = workloadInput }

    fun labels(labels: Map<String, String>) = apply { this.labels = labels }

    fun logPath(logPath: String) = apply { this.logPath = logPath }

    fun build() =
      LauncherInputMessage(
        workloadId = workloadId!!,
        workloadInput = workloadInput!!,
        labels = labels!!,
        logPath = logPath!!,
      )
  }
}
