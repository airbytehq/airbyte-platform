/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models.release

import java.time.OffsetDateTime

/** A published Airbyte application release. [publishedAt] is when chart publication completed. */
data class AirbyteRelease(
  val version: String,
  val sourceCommit: String,
  val publishedAt: OffsetDateTime,
)

class InvalidAirbyteReleaseException(
  message: String,
) : RuntimeException(message)

class AirbyteReleaseConflictException(
  val version: String,
) : RuntimeException("Release $version is already recorded with a different source commit")
