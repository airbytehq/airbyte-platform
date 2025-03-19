/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.commons.version.AirbyteVersion
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.statsd.StatsdMeterRegistry
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
    val airbyteVersion =
      mockk<AirbyteVersion> {
        every { serialize() } returns version
      }
    val configurer = StatsDRegistryConfigurer(applicationName = service, airbyteVersion = airbyteVersion)
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
    val airbyteVersion =
      mockk<AirbyteVersion> {
        every { serialize() } returns version
      }
    val configurer = StatsDRegistryConfigurer(applicationName = service, airbyteVersion = airbyteVersion)
    configurer.configure(meterRegistry)

    verify(exactly = 1) { config.commonTags(SERVICE_TAG, service, VERSION_TAG, version) }
  }

  @Test
  fun testConfigureNullMeterRegistry() {
    val service = "test-service"
    val version = "1.0.0"
    val airbyteVersion =
      mockk<AirbyteVersion> {
        every { serialize() } returns version
      }
    val configurer = StatsDRegistryConfigurer(applicationName = service, airbyteVersion = airbyteVersion)
    assertDoesNotThrow { configurer.configure(null) }
  }
}
