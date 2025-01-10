/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.ShouldFailSyncIfHeartbeatFailure;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class HeartBeatTimeoutChaperoneTest {

  private final HeartbeatMonitor heartbeatMonitor = mock(HeartbeatMonitor.class);
  private final Duration timeoutCheckDuration = Duration.ofMillis(1);

  private final FeatureFlagClient featureFlagClient = mock(TestClient.class);
  private final UUID workspaceId = UUID.randomUUID();
  private final UUID connectionId = UUID.randomUUID();
  private final MetricClient metricClient = mock(MetricClient.class);

  @Test
  void testFailHeartbeat() {
    when(featureFlagClient.boolVariation(eq(ShouldFailSyncIfHeartbeatFailure.INSTANCE), any())).thenReturn(true);
    when(heartbeatMonitor.getHeartbeatFreshnessThreshold()).thenReturn(Duration.ofSeconds(1));

    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(
        heartbeatMonitor,
        timeoutCheckDuration,
        featureFlagClient,
        workspaceId,
        Optional.of(() -> {}),
        connectionId,
        metricClient);

    final var thrown = assertThrows(HeartbeatTimeoutChaperone.HeartbeatTimeoutException.class,
        () -> heartbeatTimeoutChaperone.runWithHeartbeatThread(CompletableFuture.runAsync(() -> {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        })));

    assertEquals("Last record seen 0 seconds ago, exceeding the threshold of 1 second.", thrown.getMessage());

    verify(metricClient, times(1)).count(OssMetricsRegistry.SOURCE_HEARTBEAT_FAILURE, 1,
        new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
        new MetricAttribute(MetricTags.KILLED, "true"),
        new MetricAttribute(MetricTags.SOURCE_IMAGE, "docker image"));
  }

  @Test
  void testNotFailingHeartbeat() {
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(
        heartbeatMonitor,
        timeoutCheckDuration,
        featureFlagClient,
        workspaceId,
        Optional.of(() -> {
          try {
            Thread.sleep(Long.MAX_VALUE);
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        }),
        connectionId,
        metricClient);
    assertDoesNotThrow(() -> heartbeatTimeoutChaperone.runWithHeartbeatThread(CompletableFuture.runAsync(() -> {})));
  }

  @Test
  void testNotFailingHeartbeatIfFalseFlag() {
    when(featureFlagClient.boolVariation(eq(ShouldFailSyncIfHeartbeatFailure.INSTANCE), any())).thenReturn(false);
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(
        heartbeatMonitor,
        timeoutCheckDuration,
        featureFlagClient,
        workspaceId,
        Optional.of(() -> {}),
        connectionId,
        metricClient);
    assertDoesNotThrow(() -> heartbeatTimeoutChaperone.runWithHeartbeatThread(CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    })));
  }

  @Test
  void testMonitor() {
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(
        heartbeatMonitor,
        timeoutCheckDuration,
        featureFlagClient,
        workspaceId,
        connectionId,
        "docker image",
        metricClient);
    when(featureFlagClient.boolVariation(eq(ShouldFailSyncIfHeartbeatFailure.INSTANCE), any())).thenReturn(true);
    when(heartbeatMonitor.isBeating()).thenReturn(Optional.of(false));
    assertDoesNotThrow(() -> CompletableFuture.runAsync(heartbeatTimeoutChaperone::monitor).get(1000, TimeUnit.MILLISECONDS));
  }

  @Test
  void testMonitorDontFailIfDontStopBeating() {
    final HeartbeatTimeoutChaperone heartbeatTimeoutChaperone = new HeartbeatTimeoutChaperone(
        heartbeatMonitor,
        timeoutCheckDuration,
        featureFlagClient,
        workspaceId,
        connectionId,
        "docker image",
        metricClient);
    when(featureFlagClient.boolVariation(eq(ShouldFailSyncIfHeartbeatFailure.INSTANCE), any())).thenReturn(false);
    when(heartbeatMonitor.isBeating()).thenReturn(Optional.of(true), Optional.of(false));

    assertDoesNotThrow(() -> CompletableFuture.runAsync(heartbeatTimeoutChaperone::monitor).get(1000, TimeUnit.MILLISECONDS));
  }

}
