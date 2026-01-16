/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.commons.version.AirbyteVersion
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.registry.otlp.OtlpMeterRegistry
import io.micronaut.runtime.ApplicationConfiguration
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.opentelemetry.semconv.ServiceAttributes
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.extension.ExtendWith
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables
import uk.org.webcompere.systemstubs.jupiter.SystemStub
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension

@ExtendWith(SystemStubsExtension::class)
internal class OtlpRegistryConfigurerTest {
  @SystemStub
  val environmentVariables = EnvironmentVariables()

  @Test
  fun testConfigureWithEnvVar() {
    val applicationName = "test-app"
    val serviceName = "service-name"
    val version = "1.5.0"
    environmentVariables.set(OTEL_SERVICE_NAME_ENV_VAR, serviceName)
    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: OtlpMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(applicationName)
    val configurer = OtlpRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    configurer.configure(meterRegistry)
    verify(exactly = 1) {
      config.commonTags(
        ServiceAttributes.SERVICE_NAME.key,
        serviceName,
        SERVICE_TAG,
        serviceName,
        ServiceAttributes.SERVICE_VERSION.key,
        version,
        VERSION_TAG,
        version,
      )
    }
  }

  @Test
  fun testConfigureWithoutEnvVar() {
    val applicationName = "test-app"
    val version = "1.5.0"
    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: OtlpMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(applicationName)
    val configurer = OtlpRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    configurer.configure(meterRegistry)
    verify(exactly = 1) {
      config.commonTags(
        ServiceAttributes.SERVICE_NAME.key,
        applicationName,
        SERVICE_TAG,
        applicationName,
        ServiceAttributes.SERVICE_VERSION.key,
        version,
        VERSION_TAG,
        version,
      )
    }
  }

  @Test
  fun testConfigureNullMeterRegistry() {
    val applicationName = "test-app"
    val version = "1.5.0"
    val airbyteVersion: AirbyteVersion =
      mockk {
        every { serialize() } returns version
      }
    val applicationConfiguration = ApplicationConfiguration()
    applicationConfiguration.setName(applicationName)
    val configurer = OtlpRegistryConfigurer(airbyteVersion = airbyteVersion, applicationConfiguration = applicationConfiguration)
    assertDoesNotThrow { configurer.configure(null) }
  }
}
