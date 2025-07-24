/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.constants

/**
 * Collection of constants related to Airbyte secrets defined in connector configurations.
 */
object AirbyteSecretConstants {
  /**
   * The name of a configuration property field that has been identified as a secret.
   */
  const val AIRBYTE_SECRET_FIELD: String = "airbyte_secret"

  /**
   * A prefix to return before all secret coordinate response values.
   */
  const val AIRBYTE_SECRET_COORDINATE_PREFIX: String = "secret_coordinate::"

  /**
   * Mask value that is displayed in place of a value associated with an airbyte secret.
   */
  const val SECRETS_MASK: String = "**********"
}
