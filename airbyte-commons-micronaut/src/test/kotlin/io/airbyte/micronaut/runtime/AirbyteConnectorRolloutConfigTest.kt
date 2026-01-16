/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.micronaut.runtime

import io.micronaut.context.env.Environment
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest(environments = [Environment.TEST])
internal class AirbyteConnectorRolloutConfigDefaultTest {
  @Inject
  private lateinit var airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("", airbyteConnectorRolloutConfig.gcs.applicationCredentials)
    assertEquals("", airbyteConnectorRolloutConfig.gcs.bucketName)
    assertEquals("", airbyteConnectorRolloutConfig.gcs.projectId)
    assertEquals("", airbyteConnectorRolloutConfig.gcs.objectPrefix)
    assertEquals(DEFAULT_CONNECTOR_ROLLOUT_EXPIRATION_SECONDS, airbyteConnectorRolloutConfig.timeouts.rolloutExpirationSeconds)
    assertEquals(DEFAULT_CONNECTOR_ROLLOUT_WAIT_TIME_BETWEEN_ROLLOUT_SECONDS, airbyteConnectorRolloutConfig.timeouts.waitBetweenRolloutSeconds)
    assertEquals(
      DEFAULT_CONNECTOR_ROLLOUT_WAIT_BETWEEN_SYNC_RESULTS_QUERIES_SECONDS,
      airbyteConnectorRolloutConfig.timeouts.waitBetweenSyncResultsQueriesSeconds,
    )
  }
}

@MicronautTest(propertySources = ["classpath:application-connector-rollout.yml"])
internal class AirbyteConnectorRolloutConfigOverridesTest {
  @Inject
  private lateinit var airbyteConnectorRolloutConfig: AirbyteConnectorRolloutConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals("test-application-credentials", airbyteConnectorRolloutConfig.gcs.applicationCredentials)
    assertEquals("test-bucket-name", airbyteConnectorRolloutConfig.gcs.bucketName)
    assertEquals("test-project-id", airbyteConnectorRolloutConfig.gcs.projectId)
    assertEquals("test-object-prefix", airbyteConnectorRolloutConfig.gcs.objectPrefix)
    assertEquals(30, airbyteConnectorRolloutConfig.timeouts.rolloutExpirationSeconds)
    assertEquals(10, airbyteConnectorRolloutConfig.timeouts.waitBetweenRolloutSeconds)
    assertEquals(20, airbyteConnectorRolloutConfig.timeouts.waitBetweenSyncResultsQueriesSeconds)
  }
}
