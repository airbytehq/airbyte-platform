/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.config

import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton

/**
 * Bean Factory for common Airbyte Configuration.
 */
@Factory
class AirbyteConfigurationBeanFactory {
  @Singleton
  fun airbyteVersion(airbyteConfig: AirbyteConfig): AirbyteVersion = AirbyteVersion(airbyteConfig.version)

  /**
   * Fetch the configured edition of the Airbyte instance. Defaults to COMMUNITY.
   */
  @Singleton
  fun airbyteEdition(airbyteConfig: AirbyteConfig): AirbyteEdition = airbyteConfig.edition

  @Singleton
  @Requires(property = "airbyte.protocol.min-version")
  fun airbyteProtocolVersionRange(airbyteConfig: AirbyteConfig): AirbyteProtocolVersionRange =
    AirbyteProtocolVersionRange(Version(airbyteConfig.protocol.minVersion), Version(airbyteConfig.protocol.maxVersion))
}
