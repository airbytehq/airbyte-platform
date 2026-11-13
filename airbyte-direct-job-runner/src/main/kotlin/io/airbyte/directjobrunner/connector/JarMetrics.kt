/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.connector

import org.eclipse.aether.resolution.ArtifactResolutionException
import org.eclipse.aether.transfer.ArtifactNotFoundException
import org.eclipse.aether.transfer.ChecksumFailureException

// Tag values shared by the jar metrics.
internal const val PURPOSE_PREFETCH = "prefetch"
internal const val PURPOSE_CHECK = "check"
internal const val RESULT_SUCCESS = "success"
internal const val RESULT_TIMEOUT = "timeout"

/**
 * The `connector_image` tag for a jar's coordinates: `airbyte/source-postgres:3.8.10` for
 * `io.airbyte.connectors:source-postgres:3.8.10`, the image the jar was published for. The same tag
 * `direct_check_connector_duration` carries, so the jar metrics join with it.
 */
internal fun connectorImageTag(coordinates: String): String {
  val (_, artifactId, version) = coordinates.split(':')
  return "airbyte/$artifactId:$version"
}

/**
 * The `result` tag for a download that failed: `not_found` when the jar repository doesn't have the version,
 * `checksum_mismatch` when the downloaded file doesn't match its published checksum, `error` for anything else
 * (an unreachable repository, a stalled transfer).
 */
internal fun downloadFailureResult(e: ArtifactResolutionException): String {
  val failures =
    (sequenceOf<Throwable>(e) + e.results.asSequence().flatMap { it.exceptions.asSequence() })
      .flatMap { generateSequence(it) { failure -> failure.cause } }
      .toList()
  return when {
    failures.any { it is ChecksumFailureException } -> "checksum_mismatch"
    failures.any { it is ArtifactNotFoundException } -> "not_found"
    else -> "error"
  }
}
