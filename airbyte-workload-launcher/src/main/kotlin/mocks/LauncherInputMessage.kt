package io.airbyte.workload.launcher.mocks

class LauncherInputMessage(val workloadId: String, val workloadInput: String) {
  override fun toString(): String {
    return "WorkloadId $workloadId, workloadInput $workloadInput"
  }

  override fun equals(other: Any?): Boolean {
    if (this === other) return true
    if (javaClass != other?.javaClass) return false

    other as LauncherInputMessage

    if (workloadId != other.workloadId) return false
    if (workloadInput != other.workloadInput) return false

    return true
  }

  override fun hashCode(): Int {
    var result = workloadId.hashCode()
    result = 31 * result + workloadInput.hashCode()
    return result
  }
}
