/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
  @Throws(DatabaseCheckException::class)
  fun check()
}
