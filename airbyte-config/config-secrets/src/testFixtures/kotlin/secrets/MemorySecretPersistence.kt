/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import io.airbyte.config.secrets.persistence.SecretPersistence

/**
 * Map-based implementation of a [SecretPersistence] used for unit testing.
 */
class MemorySecretPersistence : SecretPersistence {
  private val secretMap: MutableMap<SecretCoordinate, String> = mutableMapOf()

  override fun read(coordinate: SecretCoordinate): String {
    return secretMap[coordinate] ?: ""
  }

  override fun write(
    coordinate: SecretCoordinate,
    payload: String,
  ) {
    secretMap[coordinate] = payload
  }

  override fun delete(coordinate: SecretCoordinate) {
    secretMap.remove(coordinate)
  }

  override fun disable(coordinate: SecretCoordinate) {
    // Mimic the behavior of the real implementation.
    secretMap.remove(coordinate)
  }

  val map: Map<SecretCoordinate, String>
    get() = secretMap.toMutableMap()
}
