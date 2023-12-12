/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_ACCEPT_TIMEOUT;
import static io.airbyte.metrics.lib.OssMetricsRegistry.WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class DestinationTimeoutMonitorTest {

  private final MetricClient metricClient = mock(MetricClient.class);

  @Test
  void testNoTimeout() {
    DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofMinutes(5),
        true,
        Duration.ofMillis(500));

    destinationTimeoutMonitor.startAcceptTimer();
    destinationTimeoutMonitor.startNotifyEndOfInputTimer();

    assertDoesNotThrow(() -> destinationTimeoutMonitor.runWithTimeoutThread(CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(2000);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    })));

    verify(metricClient, never()).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), anyLong(), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), anyLong(), any(MetricAttribute.class));
  }

  @Test
  void testAcceptTimeout() {
    DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        true,
        Duration.ofSeconds(1));

    destinationTimeoutMonitor.startAcceptTimer();

    assertThrows(
        DestinationTimeoutMonitor.TimeoutException.class,
        () -> destinationTimeoutMonitor.runWithTimeoutThread(CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        })));

    verify(metricClient).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), eq(1L), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), anyLong(), any(MetricAttribute.class));
  }

  @Test
  void testNotifyEndOfInputTimeout() {
    DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        true,
        Duration.ofSeconds(1));

    destinationTimeoutMonitor.startNotifyEndOfInputTimer();

    assertThrows(
        DestinationTimeoutMonitor.TimeoutException.class,
        () -> destinationTimeoutMonitor.runWithTimeoutThread(CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        })));

    verify(metricClient, never()).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), anyLong(), any(MetricAttribute.class));
    verify(metricClient).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), eq(1L), any(MetricAttribute.class));
  }

  @Test
  void testTimeoutNoException() {
    DestinationTimeoutMonitor destinationTimeoutMonitor = new DestinationTimeoutMonitor(
        UUID.randomUUID(),
        UUID.randomUUID(),
        metricClient,
        Duration.ofSeconds(1),
        false,
        Duration.ofSeconds(1));

    destinationTimeoutMonitor.startAcceptTimer();

    assertDoesNotThrow(
        () -> destinationTimeoutMonitor.runWithTimeoutThread(CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        })));

    verify(metricClient).count(eq(WORKER_DESTINATION_ACCEPT_TIMEOUT), eq(1L), any(MetricAttribute.class));
    verify(metricClient, never()).count(eq(WORKER_DESTINATION_NOTIFY_END_OF_INPUT_TIMEOUT), anyLong(), any(MetricAttribute.class));
  }

}
