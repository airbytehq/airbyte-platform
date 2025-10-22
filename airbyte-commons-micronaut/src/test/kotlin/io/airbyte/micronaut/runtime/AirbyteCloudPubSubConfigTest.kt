/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteCloudPubSubConfigDefaultTest {
  @Inject
  private lateinit var airbyteCloudPubSubConfig: AirbyteCloudPubSubConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(false, airbyteCloudPubSubConfig.enabled)
    assertEquals(JobErrorReportingStrategy.LOGGING, airbyteCloudPubSubConfig.errorReporting.strategy)
    assertEquals("", airbyteCloudPubSubConfig.errorReporting.sentry.dsn)
    assertEquals(DEFAULT_CLOUD_PUBSUB_MESSAGE_COUNT_BATCH_SIZE, airbyteCloudPubSubConfig.messageCountBatchSize)
    assertEquals(DEFAULT_CLOUD_PUBSUB_PUBLISH_DELAY_THRESHOLD_MS, airbyteCloudPubSubConfig.publishDelayThresholdMs)
    assertEquals(DEFAULT_CLOUD_PUBSUB_REQUEST_BYTES_THRESHOLD, airbyteCloudPubSubConfig.requestBytesThreshold)
    assertEquals("", airbyteCloudPubSubConfig.topic)
  }
}

@MicronautTest(propertySources = ["classpath:application-cloud-pubsub.yml"])
internal class AirbyteCloudPubSubConfigOverridesTest {
  @Inject
  private lateinit var airbyteCloudPubSubConfig: AirbyteCloudPubSubConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(true, airbyteCloudPubSubConfig.enabled)
    assertEquals(JobErrorReportingStrategy.SENTRY, airbyteCloudPubSubConfig.errorReporting.strategy)
    assertEquals("test-dsn", airbyteCloudPubSubConfig.errorReporting.sentry.dsn)
    assertEquals(2L, airbyteCloudPubSubConfig.messageCountBatchSize)
    assertEquals(3L, airbyteCloudPubSubConfig.publishDelayThresholdMs)
    assertEquals(1L, airbyteCloudPubSubConfig.requestBytesThreshold)
    assertEquals("test-topic", airbyteCloudPubSubConfig.topic)
  }
}
