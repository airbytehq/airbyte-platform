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
internal class AirbyteWorkloadLauncherConfigTest {
  @Inject
  private lateinit var airbyteWorkloadLauncherConfig: AirbyteWorkloadLauncherConfig

  @Test
  fun testLoadingDefaultValues() {
    assertEquals(Duration.parse(DEFAULT_WORKLOAD_LAUNCHER_HEARTBEAT_RATE), airbyteWorkloadLauncherConfig.heartbeatRate)
    assertEquals(Duration.parse(DEFAULT_WORKLOAD_LAUNCHER_WORKLOAD_START_TIMEOUT), airbyteWorkloadLauncherConfig.workloadStartTimeout)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_NETWORK_POLICY_INTROSPECTION, airbyteWorkloadLauncherConfig.networkPolicyIntrospection)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM, airbyteWorkloadLauncherConfig.parallelism.defaultQueue)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM, airbyteWorkloadLauncherConfig.parallelism.highPriorityQueue)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_PARALLELISM, airbyteWorkloadLauncherConfig.parallelism.maxSurge)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_QUEUE_TASK_CAP, airbyteWorkloadLauncherConfig.consumer.queueTaskCap)
    assertEquals(DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_SIZE_ITEMS, airbyteWorkloadLauncherConfig.consumer.defaultQueue.pollSizeItems)
    assertEquals(
      DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_INTERVAL_SECONDS,
      airbyteWorkloadLauncherConfig.consumer.defaultQueue.pollIntervalSeconds,
    )
    assertEquals(
      DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_SIZE_ITEMS,
      airbyteWorkloadLauncherConfig.consumer.highPriorityQueue.pollSizeItems,
    )
    assertEquals(
      DEFAULT_WORKLOAD_LAUNCHER_QUEUE_CONSUMER_POLL_INTERVAL_SECONDS,
      airbyteWorkloadLauncherConfig.consumer.highPriorityQueue.pollIntervalSeconds,
    )
  }
}

@MicronautTest(propertySources = ["classpath:application-workload-launcher.yml"])
internal class AirbyteWorkloadLauncherConfigOverridesTest {
  @Inject
  private lateinit var airbyteWorkloadLauncherConfig: AirbyteWorkloadLauncherConfig

  @Test
  fun testLoadingValuesFromConfig() {
    assertEquals(Duration.parse("PT10S"), airbyteWorkloadLauncherConfig.heartbeatRate)
    assertEquals(Duration.parse("PT1H"), airbyteWorkloadLauncherConfig.workloadStartTimeout)
    assertEquals(true, airbyteWorkloadLauncherConfig.networkPolicyIntrospection)
    assertEquals(1, airbyteWorkloadLauncherConfig.parallelism.defaultQueue)
    assertEquals(10, airbyteWorkloadLauncherConfig.parallelism.highPriorityQueue)
    assertEquals(100, airbyteWorkloadLauncherConfig.parallelism.maxSurge)
    assertEquals(100, airbyteWorkloadLauncherConfig.consumer.queueTaskCap)
    assertEquals(20, airbyteWorkloadLauncherConfig.consumer.defaultQueue.pollSizeItems)
    assertEquals(5, airbyteWorkloadLauncherConfig.consumer.defaultQueue.pollIntervalSeconds)
    assertEquals(20, airbyteWorkloadLauncherConfig.consumer.highPriorityQueue.pollSizeItems)
    assertEquals(5, airbyteWorkloadLauncherConfig.consumer.highPriorityQueue.pollIntervalSeconds)
  }
}
