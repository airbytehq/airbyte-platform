/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets.persistence

/**
 * Validates that a runtime secret-storage config contains everything its persistence type's
 * `fromSecretPersistenceConfig` factory will read as non-null, so malformed configs can be
 * rejected when the storage is created instead of failing at first use.
 *
 * Implementations report facts only — callers decide how to surface them (e.g. as HTTP errors).
 */
fun interface RuntimeConfigValidator {
  fun validate(config: Map<String, String>): RuntimeConfigError?
}

sealed interface RuntimeConfigError {
  /** Required keys that are absent or blank in the config. */
  data class MissingKeys(
    val keys: List<String>,
  ) : RuntimeConfigError

  /** A discriminator key (e.g. AWS `auth_type`) has a value outside the supported set. */
  data class InvalidValue(
    val key: String,
    val value: String,
    val validValues: List<String>,
  ) : RuntimeConfigError
}

internal fun missingKeysError(
  config: Map<String, String>,
  requiredKeys: Set<String>,
): RuntimeConfigError.MissingKeys? =
  requiredKeys
    .filter { config[it].isNullOrBlank() }
    .sorted()
    .takeIf { it.isNotEmpty() }
    ?.let { RuntimeConfigError.MissingKeys(it) }
