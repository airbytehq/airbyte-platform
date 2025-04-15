/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.init

/**
 * Custom exception that represents a failure that occurs during an attempt to initialize a database.
 */
class DatabaseInitializationException : Exception {
  constructor(message: String?) : super(message)

  constructor(message: String?, cause: Throwable?) : super(message, cause)
}
