/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

/**
 * Flag is a sealed class that all feature-flags must inherit from.
 *
 * There are two types of feature-flags; permanent and temporary. Permanent flags should inherit from the Permanent
 * while temporary flags should inherit from the Temporary class.
 *
 * No flag should directly implement this class!
 *
 * @param [key] is the globally unique identifier for identifying this specific feature-flag.
 * @param [default] is the default value of the flag.
 * @param [attrs] optional attributes associated with this flag
 */
sealed class Flag<T>(
  val key: String,
  val default: T,
  internal val attrs: Map<String, String> = mapOf(),
)

/**
 * Temporary is an open class (non-final) that all temporary feature-flags should inherit from.
 *
 * A Temporary feature-flag is any feature-flag that is not intended to exist forever.
 * Most feature-flags should be considered temporary.
 *
 * @param [key] is the globally unique identifier for identifying this specific feature-flag.
 * @param [default] is the default value of the flag.
 * @param [attrs] attributes associated with this flag
 */
open class Temporary<T>
  @JvmOverloads
  constructor(
    key: String,
    default: T,
    attrs: Map<String, String> = mapOf(),
  ) : Flag<T>(key = key, default = default, attrs = attrs)

/**
 * Permanent is an open class (non-final) that all permanent feature-flags should inherit from.
 *
 * A permanent feature-flag is any feature-flag that is intended to exist forever.
 * Few feature-flags should be considered permanent.
 *
 * @param [key] is the globally unique identifier for identifying this specific feature-flag.
 * @param [default] is the default value of the flag.
 * @param [attrs] attributes associated with this flag
 */
open class Permanent<T>
  @JvmOverloads
  constructor(
    key: String,
    default: T,
    attrs: Map<String, String> = mapOf(),
  ) : Flag<T>(key = key, default = default, attrs = attrs)

/**
 * Environment-Variable based feature-flag.
 *
 * Intended only to be used in a transitory manner as the platform migrates to an official feature-flag solution.
 * Every instance of this class should be migrated over to the Temporary class.
 *
 * @param [envVar] the environment variable to check for the status of this flag
 * @param [default] the default value of this flag, if the environment variable is not defined
 * @param [attrs] attributes associated with this flag
 */
open class EnvVar
  @JvmOverloads
  constructor(
    envVar: String,
    default: Boolean = false,
    attrs: Map<String, String> = mapOf(),
  ) : Flag<Boolean>(key = envVar, default = default, attrs = attrs) {
    /**
     * Function used to retrieve the environment-variable, overrideable for testing purposes only.
     *
     * This is internal so that it can be modified for unit-testing purposes only!
     */
    internal var fetcher: (String) -> String? = { s -> System.getenv(s) }

    /**
     * Returns true if, and only if, the environment-variable is defined and evaluates to "true".  Otherwise, returns false.
     */
    internal open fun enabled(ctx: Context): Boolean =
      fetcher(key)
        ?.takeIf { it.isNotEmpty() }
        ?.toBoolean()
        ?: default
  }
