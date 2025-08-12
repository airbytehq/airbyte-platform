/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.config

import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

/**
 * Bean Factory for common Airbyte Configuration.
 */
@Factory
class AirbyteConfigurationBeanFactory {
  @Singleton
  fun airbyteVersion(
    @Value("\${airbyte.version}") airbyteVersion: String,
  ): AirbyteVersion = AirbyteVersion(airbyteVersion)

  /**
   * Fetch the configured edition of the Airbyte instance. Defaults to COMMUNITY.
   */
  @Singleton
  fun airbyteEdition(
    @Value("\${airbyte.edition:COMMUNITY}") airbyteEdition: String,
  ): AirbyteEdition =
    runCatching { AirbyteEdition.valueOf(airbyteEdition.uppercase()) }
      .getOrDefault(AirbyteEdition.COMMUNITY)

  @Singleton
  @Requires(property = "airbyte.protocol.min-version")
  fun airbyteProtocolVersionRange(
    @Value("\${airbyte.protocol.min-version}") minVersion: String,
    @Value("\${airbyte.protocol.max-version}") maxVersion: String,
  ): AirbyteProtocolVersionRange = AirbyteProtocolVersionRange(Version(minVersion), Version(maxVersion))
}
