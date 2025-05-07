/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.io

import io.airbyte.featureflag.DestinationTimeoutSeconds
import io.airbyte.featureflag.ShouldFailSyncOnDestinationTimeout
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.context.ReplicationInputFeatureFlagReader
import io.micrometer.core.instrument.Counter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture

internal class DestinationTimeoutMonitorTest {
  private val metricClient: MetricClient = mockk()

  @Test
  fun testNoTimeout() {
    every {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    } returns mockk<Counter>()
    every {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    } returns mockk<Counter>()

    val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader =
      mockk {
        every { read(DestinationTimeoutSeconds) } returns Duration.ofMinutes(5).toSeconds().toInt()
        every { read(ShouldFailSyncOnDestinationTimeout) } returns true
      }
    val replicationInput = ReplicationInput().withWorkspaceId(UUID.randomUUID()).withConnectionId(UUID.randomUUID())

    val destinationTimeoutMonitor =
      DestinationTimeoutMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        metricClient = metricClient,
        pollInterval = Duration.ofMinutes(5),
      )

    destinationTimeoutMonitor.startAcceptTimer()
    destinationTimeoutMonitor.startNotifyEndOfInputTimer()

    assertDoesNotThrow({
      destinationTimeoutMonitor.runWithTimeoutThread(
        CompletableFuture.runAsync({
          try {
            Thread.sleep(2000)
          } catch (e: InterruptedException) {
            throw RuntimeException(e)
          }
        }),
      )
    })

    verify(exactly = 0) {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    }
    verify(exactly = 0) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    }
  }

  @Test
  fun testAcceptTimeout() {
    every {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    } returns mockk<Counter>()
    every {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    } returns mockk<Counter>()

    val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader =
      mockk {
        every { read(DestinationTimeoutSeconds) } returns Duration.ofSeconds(1).toSeconds().toInt()
        every { read(ShouldFailSyncOnDestinationTimeout) } returns true
      }

    val replicationInput = ReplicationInput().withWorkspaceId(UUID.randomUUID()).withConnectionId(UUID.randomUUID())

    val destinationTimeoutMonitor =
      DestinationTimeoutMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        metricClient = metricClient,
        pollInterval = Duration.ofSeconds(1),
      )

    destinationTimeoutMonitor.startAcceptTimer()

    assertThrows(
      DestinationTimeoutMonitor.TimeoutException::class.java,
      {
        destinationTimeoutMonitor.runWithTimeoutThread(
          CompletableFuture.runAsync({
            try {
              Thread.sleep(Long.Companion.MAX_VALUE)
            } catch (e: InterruptedException) {
              throw RuntimeException(e)
            }
          }),
        )
      },
    )

    verify(exactly = 1) {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    }
    verify(exactly = 0) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    }
  }

  @Test
  fun testNotifyEndOfInputTimeout() {
    every {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    } returns mockk<Counter>()
    every {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    } returns mockk<Counter>()

    val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader =
      mockk {
        every { read(DestinationTimeoutSeconds) } returns Duration.ofSeconds(1).toSeconds().toInt()
        every { read(ShouldFailSyncOnDestinationTimeout) } returns true
      }
    val replicationInput = ReplicationInput().withWorkspaceId(UUID.randomUUID()).withConnectionId(UUID.randomUUID())

    val destinationTimeoutMonitor =
      DestinationTimeoutMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        metricClient = metricClient,
        pollInterval = Duration.ofSeconds(1),
      )

    destinationTimeoutMonitor.startNotifyEndOfInputTimer()

    assertThrows<DestinationTimeoutMonitor.TimeoutException?>(
      DestinationTimeoutMonitor.TimeoutException::class.java,
      {
        destinationTimeoutMonitor.runWithTimeoutThread(
          CompletableFuture.runAsync({
            try {
              Thread.sleep(Long.Companion.MAX_VALUE)
            } catch (e: InterruptedException) {
              throw RuntimeException(e)
            }
          }),
        )
      },
    )

    verify(exactly = 0) {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    }
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    }
  }

  @Test
  fun testTimeoutNoException() {
    every {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    } returns mockk<Counter>()
    every {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    } returns mockk<Counter>()

    val replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader =
      mockk {
        every { read(DestinationTimeoutSeconds) } returns Duration.ofSeconds(1).toSeconds().toInt()
        every { read(ShouldFailSyncOnDestinationTimeout) } returns false
      }
    val replicationInput = ReplicationInput().withWorkspaceId(UUID.randomUUID()).withConnectionId(UUID.randomUUID())

    val destinationTimeoutMonitor =
      DestinationTimeoutMonitor(
        replicationInput = replicationInput,
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        metricClient = metricClient,
        pollInterval = Duration.ofSeconds(1),
      )

    destinationTimeoutMonitor.startAcceptTimer()

    assertDoesNotThrow(
      {
        destinationTimeoutMonitor.runWithTimeoutThread(
          CompletableFuture.runAsync({
            try {
              Thread.sleep(Long.Companion.MAX_VALUE)
            } catch (e: InterruptedException) {
              throw RuntimeException(e)
            }
          }),
        )
      },
    )

    verify(exactly = 1) {
      metricClient.count(metric = OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT, value = any(), attributes = anyVararg<MetricAttribute>())
    }
    verify(exactly = 0) {
      metricClient.count(
        metric = OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT,
        value = any(),
        attributes = anyVararg<MetricAttribute>(),
      )
    }
  }
}
