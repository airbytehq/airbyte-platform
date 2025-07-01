/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

import dev.failsafe.RetryPolicy
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.launcher.PodSweeper
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

@EnableKubernetesMockClient(crud = true)
class PodSweeperTest {
  private val mockRetryPolicy: RetryPolicy<Any> = RetryPolicy.ofDefaults()
  private val mockMetricClient: MetricClient = mockk(relaxed = true)

  private lateinit var client: KubernetesClient

  @Test
  fun `test no pods - nothing to delete`() {
    // Arrange
    val sweeper = podSweeper(10, 5, 30)

    // Act
    sweeper.sweepPods()

    // Assert
    // No exceptions or logs about pods. Just ensure no crash.
  }

  @Test
  fun `test running pod older than RUNNING_TTL gets deleted`() {
    // Arrange
    val ttlMinutes = 60L
    val now = Instant.now()

    // Suppose the last transition time is 2 hours ago
    val twoHoursAgo = now.minus(Duration.ofHours(2))
    val timeString =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .withZone(ZoneOffset.UTC)
        .format(twoHoursAgo)

    // Create a "Running" pod with the airbyte=job-pod label
    val runningPod: Pod =
      PodBuilder()
        .withNewMetadata()
        .withName("my-running-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        // We'll set .conditions[0].lastTransitionTime
        .addNewCondition()
        .withLastTransitionTime(timeString)
        .endCondition()
        // We can also set startTime if desired
        .withStartTime(timeString)
        .endStatus()
        .build()

    // Create the pod in the mock cluster
    client
      .pods()
      .inNamespace("default")
      .resource(runningPod)
      .create()

    val sweeper = podSweeper(ttlMinutes, null, null)

    // Act
    sweeper.sweepPods()

    // Assert
    // The pod should be deleted because it's older than 60 minutes
    val stillExists =
      client
        .pods()
        .inNamespace("default")
        .withName("my-running-pod")
        .get()
    assertNull(stillExists, "Pod should have been deleted")
  }

  @Test
  fun `test running pod newer than RUNNING_TTL is NOT deleted`() {
    // Arrange
    val ttlMinutes = 60L
    val now = Instant.now()

    // Suppose last transition time is 30 minutes ago
    val halfHourAgo = now.minus(Duration.ofMinutes(30))
    val timeString =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .withZone(ZoneOffset.UTC)
        .format(halfHourAgo)

    val runningPod: Pod =
      PodBuilder()
        .withNewMetadata()
        .withName("recent-running-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        .addNewCondition()
        .withLastTransitionTime(timeString)
        .endCondition()
        .withStartTime(timeString)
        .endStatus()
        .build()

    client
      .pods()
      .inNamespace("default")
      .resource(runningPod)
      .create()

    val sweeper = podSweeper(ttlMinutes, null, null)

    // Act
    sweeper.sweepPods()

    // Assert
    // Pod should still exist because it's only 30 minutes old, which is < 60
    val stillExists =
      client
        .pods()
        .inNamespace("default")
        .withName("recent-running-pod")
        .get()
    assertNotNull(stillExists, "Pod should NOT have been deleted yet.")
  }

  @Test
  fun `test succeeded pod older than SUCCEEDED_TTL gets deleted`() {
    // Arrange
    val succeededTtlMinutes = 10L
    val now = Instant.now()

    // This pod finished 30 minutes ago
    val thirtyMinutesAgo = now.minus(Duration.ofMinutes(30))
    val timeString =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .withZone(ZoneOffset.UTC)
        .format(thirtyMinutesAgo)

    val succeededPod =
      PodBuilder()
        .withNewMetadata()
        .withName("succeeded-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Succeeded")
        .addNewCondition()
        .withLastTransitionTime(timeString)
        .endCondition()
        .withStartTime(timeString)
        .endStatus()
        .build()

    client
      .pods()
      .inNamespace("default")
      .resource(succeededPod)
      .create()

    val sweeper = podSweeper(null, succeededTtlMinutes, null)

    // Act
    sweeper.sweepPods()

    // Assert
    val stillExists =
      client
        .pods()
        .inNamespace("default")
        .withName("succeeded-pod")
        .get()
    assertNull(stillExists, "Succeeded pod older than 10 min should have been deleted")
  }

  @Test
  fun `test failed pod older than UNSUCCESSFUL_TTL gets deleted`() {
    // Arrange
    val unsuccessfulTtlMinutes = 180L // 3 hours
    val now = Instant.now()

    // Pod failed 4 hours ago
    val fourHoursAgo = now.minus(Duration.ofHours(4))
    val timeString =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME
        .withZone(ZoneOffset.UTC)
        .format(fourHoursAgo)

    val failedPod =
      PodBuilder()
        .withNewMetadata()
        .withName("failed-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Failed")
        .addNewCondition()
        .withLastTransitionTime(timeString)
        .endCondition()
        .withStartTime(timeString)
        .endStatus()
        .build()

    client
      .pods()
      .inNamespace("default")
      .resource(failedPod)
      .create()

    val sweeper = podSweeper(null, null, unsuccessfulTtlMinutes)

    // Act
    sweeper.sweepPods()

    // Assert
    val stillExists =
      client
        .pods()
        .inNamespace("default")
        .withName("failed-pod")
        .get()
    assertNull(stillExists, "Failed pod older than 3 hours should have been deleted")
  }

  @Test
  fun `test missing times or parse errors`() {
    // Pod is missing transitionTime and startTime => it should be skipped
    val noTimePod =
      PodBuilder()
        .withNewMetadata()
        .withName("no-time-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        .endStatus()
        .build()

    // Pod has a malformed date => parse error => also gets skipped
    val malformedDatePod =
      PodBuilder()
        .withNewMetadata()
        .withName("malformed-date-pod")
        .addToLabels("airbyte", "job-pod")
        .endMetadata()
        .withNewStatus()
        .withPhase("Succeeded")
        .addNewCondition()
        .withLastTransitionTime("this-is-not-an-ISO-date")
        .endCondition()
        .endStatus()
        .build()

    client
      .pods()
      .inNamespace("default")
      .resource(noTimePod)
      .create()
    client
      .pods()
      .inNamespace("default")
      .resource(malformedDatePod)
      .create()

    val sweeper = podSweeper(1, 1, 1)

    // Act
    sweeper.sweepPods()

    // Assert - They should NOT be deleted because the code can't parse or find a date
    val noTimePodCheck =
      client
        .pods()
        .inNamespace("default")
        .withName("no-time-pod")
        .get()
    assertNotNull(noTimePodCheck, "Pod with no time should remain since we cannot compare TTL")

    val malformedDatePodCheck =
      client
        .pods()
        .inNamespace("default")
        .withName("malformed-date-pod")
        .get()
    assertNotNull(malformedDatePodCheck, "Pod with malformed date should remain since we cannot parse TTL")
  }

  private fun podSweeper(
    runningTtL: Long?,
    succeededTtl: Long?,
    unSucceededTtl: Long?,
  ): PodSweeper =
    PodSweeper(
      client,
      mockMetricClient,
      Clock.systemUTC(),
      "default",
      mockRetryPolicy,
      runningTtL,
      succeededTtl,
      unSucceededTtl,
    )
}
