package io.airbyte.notification.messages

import java.time.Duration
import java.time.Instant

data class SyncSummary(
  val workspace: WorkspaceInfo,
  val connection: ConnectionInfo,
  val source: SourceInfo,
  val destination: DestinationInfo,
  val jobId: Long,
  val isSuccess: Boolean,
  val startedAt: Instant?,
  val finishedAt: Instant?,
  val bytesEmitted: Long,
  val bytesCommitted: Long,
  val recordsEmitted: Long,
  val recordsCommitted: Long,
  val recordsFilteredOut: Long = 0,
  val bytesFilteredOut: Long = 0,
  val errorMessage: String? = null,
) {
  fun getDurationInSeconds(): Long? =
    when {
      startedAt != null && finishedAt != null -> Duration.between(startedAt, finishedAt).seconds
      else -> null
    }

  fun getDurationFormatted(): String? =
    when {
      startedAt != null && finishedAt != null -> formatDuration(startedAt, finishedAt)
      else -> null
    }

  fun getBytesEmittedFormatted() = formatVolume(bytesEmitted)

  fun getBytesCommittedFormatted() = formatVolume(bytesCommitted)
}

private fun formatDuration(
  start: Instant,
  end: Instant,
): String {
  val duration = Duration.between(start, end)
  return when {
    duration.toMinutes() == 0L -> "${duration.toSecondsPart()} sec"
    duration.toHours() == 0L -> "${duration.toMinutesPart()} min ${duration.toSecondsPart()} sec"
    duration.toDays() == 0L -> "${duration.toHoursPart()} hours ${duration.toMinutesPart()} min"
    else -> "${duration.toDays()} days ${duration.toHours()} hours"
  }
}

private fun formatVolume(bytes: Long): String {
  var currentValue = bytes
  val byteLimit = 1024
  listOf("B", "kB", "MB", "GB").forEach { unit ->
    if (currentValue < byteLimit) {
      return "$currentValue $unit"
    }
    currentValue /= byteLimit
  }

  return "$currentValue TB"
}
