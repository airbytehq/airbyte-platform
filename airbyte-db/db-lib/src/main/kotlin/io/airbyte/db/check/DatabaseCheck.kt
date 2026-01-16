/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

/**
 * Defines the interface for performing checks against a database.
 */
interface DatabaseCheck {
  /**
   * Checks whether the configured database is available.
   *
   * @throws DatabaseCheckException if unable to perform the check.
   */
  fun check()
}
