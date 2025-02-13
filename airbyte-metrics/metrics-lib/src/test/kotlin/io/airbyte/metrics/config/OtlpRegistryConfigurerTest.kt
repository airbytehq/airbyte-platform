/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.registry.otlp.OtlpMeterRegistry
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
    environmentVariables.set(OTEL_SERVICE_NAME_ENV_VAR, serviceName)
    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: OtlpMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val configurer = OtlpRegistryConfigurer(applicationName)
    configurer.configure(meterRegistry)
    verify(exactly = 1) { config.commonTags(ServiceAttributes.SERVICE_NAME.key, serviceName) }
  }

  @Test
  fun testConfigureWithoutEnvVar() {
    val applicationName = "test-app"
    val config: MeterRegistry.Config =
      mockk {
        every { commonTags(*anyVararg<String>()) } returns this
      }
    val meterRegistry: OtlpMeterRegistry =
      mockk {
        every { config() } returns config
      }
    val configurer = OtlpRegistryConfigurer(applicationName)
    configurer.configure(meterRegistry)
    verify(exactly = 1) { config.commonTags(ServiceAttributes.SERVICE_NAME.key, applicationName) }
  }

  @Test
  fun testConfigureNullMeterRegistry() {
    val applicationName = "test-app"
    val configurer = OtlpRegistryConfigurer(applicationName)
    assertDoesNotThrow { configurer.configure(null) }
  }
}
