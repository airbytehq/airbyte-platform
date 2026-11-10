/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime

/**
 * Whether the release a data plane reports is still inside its support period. This is the age of the release, not
 * its compatibility with the control plane.
 */
sealed interface DataplaneVersionSupport {
  /**
   * The reported version is a published release.
   */
  data class Published(
    val publishedAt: OffsetDateTime,
    val supportedUntil: OffsetDateTime,
    val supported: Boolean,
  ) : DataplaneVersionSupport

  /**
   * The data plane reported no version, or one the release catalog doesn't list, such as a custom build.
   */
  data object Unknown : DataplaneVersionSupport
}
