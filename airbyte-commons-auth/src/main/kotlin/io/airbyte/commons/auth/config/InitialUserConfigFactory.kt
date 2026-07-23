/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.airbyte.micronaut.runtime.AirbyteAuthConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
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
  @Requires(property = "airbyte.auth.initial-user.email", pattern = ".+")
  fun defaultInitialUserConfig(airbyteAuthConfig: AirbyteAuthConfig): InitialUserConfig {
    if (airbyteAuthConfig.initialUser.email.isEmpty() || airbyteAuthConfig.initialUser.password.isEmpty()) {
      throw IllegalStateException(
        "Missing required initial user configuration. Please ensure all of the following properties are set: " +
          "airbyte.auth.initial-user.email, " +
          "airbyte.auth.initial-user.password",
      )
    }
    return InitialUserConfig(
      email = airbyteAuthConfig.initialUser.email,
      firstName = airbyteAuthConfig.initialUser.firstName,
      lastName = airbyteAuthConfig.initialUser.lastName,
      password = airbyteAuthConfig.initialUser.password,
    )
  }
}

data class InitialUserConfig(
  var email: String,
  var firstName: String?,
  var lastName: String?,
  var password: String,
)
