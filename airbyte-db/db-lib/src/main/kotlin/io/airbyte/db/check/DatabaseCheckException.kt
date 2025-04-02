/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.check

/**
 * Custom exception that represents a failure that occurs during an attempt to check the
 * availability or migration status of a database.
 */
class DatabaseCheckException : Exception {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
