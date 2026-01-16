/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteTemporalConfigDefaultTest {
  @Inject
  private lateinit var airbyteTemporalConfig: AirbyteTemporalConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteTemporalConfig.cloud.billing.host)
    assertEquals("", airbyteTemporalConfig.cloud.billing.namespace)
    assertEquals("", airbyteTemporalConfig.cloud.client.cert)
    assertEquals("", airbyteTemporalConfig.cloud.client.key)
    assertEquals("", airbyteTemporalConfig.cloud.connectorRollout.host)
    assertEquals("", airbyteTemporalConfig.cloud.connectorRollout.namespace)
    assertEquals(false, airbyteTemporalConfig.cloud.enabled)
    assertEquals("", airbyteTemporalConfig.cloud.host)
    assertEquals("", airbyteTemporalConfig.cloud.namespace)
    assertEquals(DEFAULT_TEMPORAL_HOST, airbyteTemporalConfig.host)
    assertEquals(DEFAULT_TEMPORAL_RETENTION_DAYS, airbyteTemporalConfig.retention)
    assertEquals(Duration.parse(DEFAULT_TEMPORAL_RPC_TIMEOUT_SECONDS), airbyteTemporalConfig.sdk.timeouts.rpcTimeout)
    assertEquals(Duration.parse(DEFAULT_TEMPORAL_RPC_LONG_POLL_TIMEOUT_SECONDS), airbyteTemporalConfig.sdk.timeouts.rpcLongPollTimeout)
    assertEquals(Duration.parse(DEFAULT_TEMPORAL_RPC_QUERY_TIMEOUT_SECONDS), airbyteTemporalConfig.sdk.timeouts.rpcQueryTimeout)
  }
}

@MicronautTest(propertySources = ["classpath:application-temporal.yml"])
internal class AirbyteTemporalConfigOverridesTest {
  @Inject
  private lateinit var airbyteTemporalConfig: AirbyteTemporalConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-temporal-cloud-billing-host", airbyteTemporalConfig.cloud.billing.host)
    assertEquals("test-temporal-cloud-billing-namespace", airbyteTemporalConfig.cloud.billing.namespace)
    assertEquals("test-temporal-cloud-client-cert", airbyteTemporalConfig.cloud.client.cert)
    assertEquals("test-temporal-cloud-client-key", airbyteTemporalConfig.cloud.client.key)
    assertEquals("test-temporal-cloud-connector-rollout-host", airbyteTemporalConfig.cloud.connectorRollout.host)
    assertEquals("test-temporal-cloud-connector-rollout-namespace", airbyteTemporalConfig.cloud.connectorRollout.namespace)
    assertEquals(true, airbyteTemporalConfig.cloud.enabled)
    assertEquals("test-temporal-cloud-host", airbyteTemporalConfig.cloud.host)
    assertEquals("test-temporal-cloud-namespace", airbyteTemporalConfig.cloud.namespace)
    assertEquals("test-temporal-host:7233", airbyteTemporalConfig.host)
    assertEquals(10, airbyteTemporalConfig.retention)
    assertEquals(Duration.parse("PT5S"), airbyteTemporalConfig.sdk.timeouts.rpcTimeout)
    assertEquals(Duration.parse("PT6S"), airbyteTemporalConfig.sdk.timeouts.rpcLongPollTimeout)
    assertEquals(Duration.parse("PT7S"), airbyteTemporalConfig.sdk.timeouts.rpcQueryTimeout)
  }
}
