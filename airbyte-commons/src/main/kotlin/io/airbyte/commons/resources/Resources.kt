/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.resources

object Resources {
  /**
   * Reads the [resource], returning it as a string.
   *
   * @throws [IllegalArgumentException] if [resource] does not exist
   * @return the contents of [resource]
   */
  fun read(resource: String): String =
    this::class.java.getResource("/$resource")?.readText() ?: throw IllegalArgumentException("Resource not found: $resource")

  /**
   * Attempts to read the [resource].
   *
   * @return the contents of [resource] if successful, null otherwise
   */
  fun readOrNull(resource: String): String? = this::class.java.getResource("/$resource")?.readText()
}
