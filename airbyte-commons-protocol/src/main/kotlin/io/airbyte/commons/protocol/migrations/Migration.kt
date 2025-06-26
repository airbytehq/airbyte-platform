/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.migrations

import io.airbyte.commons.version.Version

/**
 * Migration for Protocol Version.
 */
interface Migration {
  /**
   * The Old version, note that due to semver, the important piece of information is the Major.
   */
  fun getPreviousVersion(): Version

  /**
   * The New version, note that due to semver, the important piece of information is the Major.
   */
  fun getCurrentVersion(): Version
}
