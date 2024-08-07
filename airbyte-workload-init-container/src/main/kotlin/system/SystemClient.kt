package io.airbyte.initContainer.system

import jakarta.inject.Singleton

/**
 * Wraps system calls for testing.
 */
@Singleton
class SystemClient {
  fun exitProcess(code: Int) {
    exitProcess(code)
  }
}
