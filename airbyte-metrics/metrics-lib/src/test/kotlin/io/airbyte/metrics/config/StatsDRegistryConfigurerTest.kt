/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.commons.version.AirbyteVersion
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.statsd.StatsdMeterRegistry
import io.micronaut.runtime.ApplicationConfiguration
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables
import uk.org.webcompere.systemstubs.jupiter.SystemStub
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension

@ExtendWith(SystemStubsExtension::class)
internal class StatsDRegistryConfigurerTest {
  @SystemStub
  val environmentVariables = EnvironmentVariables()

  @Test
  fun testConfigure() {
    val service = "test-service"
    val version = "1.0.0"

    environmentVariables.set(DATA_DOG_SERVICE_TAG, service)
    environmentVariables.set(DATA_DOG_VERSION_TAG, version)

    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: StatsdMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(service)
    val configurer = StatsDRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    configurer.configure(meterRegistry)

    verify(exactly = 1) { config.commonTags(SERVICE_TAG, service, VERSION_TAG, version) }
  }

  @Test
  fun testConfigureMissingEnvVars() {
    val service = "test-service"
    val version = "1.0.0"

    environmentVariables.remove(DATA_DOG_SERVICE_TAG)
    environmentVariables.remove(DATA_DOG_VERSION_TAG)

    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: StatsdMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(service)
    val configurer = StatsDRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    configurer.configure(meterRegistry)

    verify(exactly = 1) { config.commonTags(SERVICE_TAG, service, VERSION_TAG, version) }
  }

  @Test
  fun testConfigureNullMeterRegistry() {
    val service = "test-service"
    val version = "1.0.0"
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(service)
    val configurer = StatsDRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    assertDoesNotThrow { configurer.configure(null) }
  }
}
