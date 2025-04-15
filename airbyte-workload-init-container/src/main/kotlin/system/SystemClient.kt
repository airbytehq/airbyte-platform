/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.initContainer.system

import jakarta.inject.Singleton

class ExitWithCode(
  val code: Int,
) : RuntimeException("Exiting with code $code")

/**
 * Wraps system calls for testing.
 */
@Singleton
class SystemClient {
  fun exitProcess(code: Int): Unit = throw ExitWithCode(code)
}
