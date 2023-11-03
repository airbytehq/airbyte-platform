package io.airbyte.config.messages

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

@JsonDeserialize(builder = LauncherInputMessage.Builder::class)
data class LauncherInputMessage(val workloadId: String, val workloadInput: String) {
  data class Builder(var workloadId: String? = null, var workloadInput: String? = null) {
    fun workloadId(workloadId: String) = apply { this.workloadId = workloadId }

    fun workloadInput(workloadInput: String) = apply { this.workloadInput = workloadInput }

    fun build() = LauncherInputMessage(workloadId = workloadId!!, workloadInput = workloadInput!!)
  }
}
