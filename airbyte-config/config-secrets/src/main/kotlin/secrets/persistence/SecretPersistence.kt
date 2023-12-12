/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

import io.airbyte.config.secrets.SecretCoordinate

/**
 * Provides a read-only interface to a backing secrets store similar to [SecretPersistence].
 * In practice, the functionality should be provided by a [SecretPersistence.read] function.
 */
fun interface ReadOnlySecretPersistence {
  fun read(coordinate: SecretCoordinate): String
}

/**
 * Provides the ability to read and write secrets to a backing store. Assumes that secret payloads
 * are always strings. See {@link SecretCoordinate} for more information on how secrets are
 * identified.
 */
interface SecretPersistence : ReadOnlySecretPersistence {
  /**
   * Performs any initialization prior to utilization of the persistence object. This exists to make
   * it possible to create instances within a dependency management framework, where any
   * initialization logic should not be present in a constructor.
   *
   * @throws Exception if unable to perform the initialization.
   */
  @Throws(Exception::class)
  fun initialize() {}

  override fun read(coordinate: SecretCoordinate): String

  fun write(
    coordinate: SecretCoordinate,
    payload: String,
  )
}
