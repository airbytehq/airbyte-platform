package io.airbyte.commons.server.config

import io.airbyte.api.model.generated.AuthConfiguration.ModeEnum
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class ConfigFactories {
  @Singleton
  @Requires(env = ["community-auth"])
  @Replaces(named = "default-auth-mode")
  fun getCommunityAuthMode(): ModeEnum {
    return ModeEnum.SIMPLE
  }

  @Singleton
  @RequiresAirbyteProEnabled
  @Replaces(named = "default-auth-mode")
  fun getEnterpriseAuthMode(): ModeEnum {
    return ModeEnum.OIDC
  }

  @Singleton
  @Named("default-auth-mode")
  fun getDefaultAuthMode(): ModeEnum {
    return ModeEnum.NONE
  }
}
