/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Factory
class InitialUserConfigFactory {
  /**
   * Returns the InitialUserConfig with values from the environment, if present. This is the preferred way
   * to configure the initial user and should take precedence over `airbyte.yml`.
   * This bean requires a non-empty `email` property so that it can remain unloaded
   * if the initial-user configuration is coming from `airbyte.yml` instead of the environment,
   * for backwards compatibility.
   */
  @Singleton
  @Primary
  @Requires(property = "airbyte.auth.initial-user.email", pattern = ".+")
  fun defaultInitialUserConfig(
    @Value("\${airbyte.auth.initial-user.email}") email: String?,
    @Value("\${airbyte.auth.initial-user.first-name}") firstName: String?,
    @Value("\${airbyte.auth.initial-user.last-name}") lastName: String?,
    @Value("\${airbyte.auth.initial-user.password}") password: String?,
  ): InitialUserConfig {
    if (email.isNullOrEmpty() || password.isNullOrEmpty()) {
      throw IllegalStateException(
        "Missing required initial user configuration. Please ensure all of the following properties are set: " +
          "airbyte.auth.initial-user.email, " +
          "airbyte.auth.initial-user.password",
      )
    }
    return InitialUserConfig(email, firstName, lastName, password)
  }

  /**
   * Returns InitialUserConfig with values from `airbyte.yml`. This is for backwards compatibility.
   * The above Primary bean is the preferred way to configure the initial user.
   */
  @Singleton
  @Requires(property = "airbyte-yml.initial-user.email", pattern = ".+")
  fun airbyteYmlInitialUserConfig(
    @Value("\${airbyte-yml.initial-user.email}") email: String?,
    @Value("\${airbyte-yml.initial-user.first-name}") firstName: String?,
    @Value("\${airbyte-yml.initial-user.last-name}") lastName: String?,
    @Value("\${airbyte-yml.initial-user.password}") password: String?,
  ): InitialUserConfig {
    if (email.isNullOrEmpty() || password.isNullOrEmpty()) {
      throw IllegalStateException(
        "Missing required initial user configuration. Please ensure all of the following properties are set: " +
          "airbyte-yml.initial-user.email, " +
          "airbyte-yml.initial-user.password",
      )
    }
    return InitialUserConfig(email, firstName, lastName, password)
  }
}

data class InitialUserConfig(
  var email: String,
  var firstName: String?,
  var lastName: String?,
  var password: String,
)
