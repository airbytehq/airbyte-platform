package io.airbyte.config.messages

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.config.WorkloadType
import java.util.UUID

@JsonDeserialize(builder = LauncherInputMessage.Builder::class)
data class LauncherInputMessage(
  val workloadId: String,
  val workloadInput: String,
  val labels: Map<String, String>,
  val logPath: String,
  val mutexKey: String?,
  val workloadType: WorkloadType,
  val startTimeMs: Long? = null,
  val autoId: UUID,
) {
  data class Builder(
    var workloadId: String? = null,
    var workloadInput: String? = null,
    var labels: Map<String, String>? = null,
    var logPath: String? = null,
    var mutexKey: String? = null,
    var workloadType: WorkloadType? = null,
    var startTimeMs: Long? = null,
    var autoId: UUID? = null,
  ) {
    fun workloadId(workloadId: String) = apply { this.workloadId = workloadId }

    fun workloadInput(workloadInput: String) = apply { this.workloadInput = workloadInput }

    fun labels(labels: Map<String, String>) = apply { this.labels = labels }

    fun logPath(logPath: String) = apply { this.logPath = logPath }

    fun startTimeMs(timestampMs: Long) = apply { this.startTimeMs = timestampMs }

    fun autoId(autoId: UUID) = apply { this.autoId = autoId }

    fun build() =
      LauncherInputMessage(
        workloadId = workloadId ?: throw IllegalArgumentException("workloadId cannot be null"),
        workloadInput = workloadInput ?: throw IllegalArgumentException("workloadInput cannot be null"),
        labels = labels ?: throw IllegalArgumentException("labels cannot be null"),
        logPath = logPath ?: throw IllegalArgumentException("logPath cannot be null"),
        workloadType = workloadType ?: throw IllegalArgumentException("workloadType cannot be null"),
        autoId = autoId ?: throw IllegalArgumentException("autoId cannot be null"),
        mutexKey = mutexKey,
        startTimeMs = startTimeMs,
      )
  }
}
