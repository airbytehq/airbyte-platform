/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.constants.ContainerConstants
import io.fabric8.kubernetes.api.model.Pod
import java.time.Duration
import java.time.Instant

internal data class PodStartupDurations(
  val createToScheduled: Duration?,
  val scheduledToInitialized: Duration?,
  val initializedToReady: Duration?,
)

internal fun extractPodStartupDurations(pod: Pod): PodStartupDurations {
  val creationTime = parseTimestamp(pod.metadata?.creationTimestamp)
  val conditions =
    pod.status
      ?.conditions
      .orEmpty()
      .filter { it.status == PodConditionStatus.TRUE }
      .associateBy { it.type }
  val scheduledTime = parseTimestamp(conditions[PodConditionType.SCHEDULED]?.lastTransitionTime)
  val initializedTime = parseTimestamp(conditions[PodConditionType.INITIALIZED]?.lastTransitionTime)
  val readyTime = parseTimestamp(conditions[PodConditionType.READY]?.lastTransitionTime)

  return PodStartupDurations(
    createToScheduled = durationBetween(creationTime, scheduledTime),
    scheduledToInitialized = durationBetween(scheduledTime, initializedTime),
    initializedToReady = durationBetween(initializedTime, readyTime),
  )
}

internal fun extractConnectorImage(pod: Pod): String =
  pod.spec
    ?.containers
    .orEmpty()
    .firstOrNull { it.name == ContainerConstants.MAIN_CONTAINER_NAME }
    ?.image
    ?.let(::normalizeConnectorImage)
    ?: MetricTags.UNKNOWN

private fun parseTimestamp(timestamp: String?): Instant? = timestamp?.let { runCatching { Instant.parse(it) }.getOrNull() }

private fun normalizeConnectorImage(image: String): String? {
  val trimmedImage = image.trim()
  if (trimmedImage.isEmpty() || trimmedImage.any(Char::isWhitespace)) {
    return null
  }

  val digestSeparatorIndex = trimmedImage.indexOf('@')
  val imageWithoutDigest =
    if (digestSeparatorIndex >= 0) {
      trimmedImage.substring(0, digestSeparatorIndex).takeIf { digestSeparatorIndex < trimmedImage.lastIndex }
    } else {
      trimmedImage
    }
  if (imageWithoutDigest.isNullOrEmpty()) {
    return null
  }

  val lastSlashIndex = imageWithoutDigest.lastIndexOf('/')
  val lastColonIndex = imageWithoutDigest.lastIndexOf(':')
  return if (lastColonIndex > lastSlashIndex) {
    imageWithoutDigest
      .substring(0, lastColonIndex)
      .takeIf { lastColonIndex < imageWithoutDigest.lastIndex && it.isNotEmpty() }
  } else {
    imageWithoutDigest
  }
}

private fun durationBetween(
  start: Instant?,
  end: Instant?,
): Duration? =
  if (start == null || end == null) {
    null
  } else {
    Duration.between(start, end).takeUnless { it.isNegative }
  }

private object PodConditionType {
  const val SCHEDULED = "PodScheduled"
  const val INITIALIZED = "Initialized"
  const val READY = "Ready"
}

private object PodConditionStatus {
  const val TRUE = "True"
}
