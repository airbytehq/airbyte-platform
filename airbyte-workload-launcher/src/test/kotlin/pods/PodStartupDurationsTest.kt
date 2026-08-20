/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.airbyte.workload.launcher.constants.ContainerConstants
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.PodConditionBuilder
import io.fabric8.kubernetes.api.model.PodSpecBuilder
import io.fabric8.kubernetes.api.model.PodStatusBuilder
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Duration

internal class PodStartupDurationsTest {
  @Test
  fun `extracts durations when all conditions are present`() {
    val pod =
      pod(
        creationTimestamp = "2026-01-01T00:00:00Z",
        scheduledTimestamp = "2026-01-01T00:00:02Z",
        initializedTimestamp = "2026-01-01T00:00:05Z",
        readyTimestamp = "2026-01-01T00:00:09Z",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = Duration.ofSeconds(2),
          scheduledToInitialized = Duration.ofSeconds(3),
          initializedToReady = Duration.ofSeconds(4),
        ),
      )
  }

  @Test
  fun `returns null for durations whose conditions are missing`() {
    val pod =
      pod(
        creationTimestamp = "2026-01-01T00:00:00Z",
        scheduledTimestamp = "2026-01-01T00:00:02Z",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = Duration.ofSeconds(2),
          scheduledToInitialized = null,
          initializedToReady = null,
        ),
      )
  }

  @Test
  fun `returns null for out of order timestamps`() {
    val pod =
      pod(
        creationTimestamp = "2026-01-01T00:00:02Z",
        scheduledTimestamp = "2026-01-01T00:00:01Z",
        initializedTimestamp = "2026-01-01T00:00:03Z",
        readyTimestamp = "2026-01-01T00:00:04Z",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = null,
          scheduledToInitialized = Duration.ofSeconds(2),
          initializedToReady = Duration.ofSeconds(1),
        ),
      )
  }

  @Test
  fun `returns zero for equal timestamps`() {
    val pod =
      pod(
        creationTimestamp = "2026-01-01T00:00:01Z",
        scheduledTimestamp = "2026-01-01T00:00:01Z",
        initializedTimestamp = "2026-01-01T00:00:01Z",
        readyTimestamp = "2026-01-01T00:00:01Z",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = Duration.ZERO,
          scheduledToInitialized = Duration.ZERO,
          initializedToReady = Duration.ZERO,
        ),
      )
  }

  @Test
  fun `returns null when creation timestamp is missing`() {
    val pod =
      pod(
        creationTimestamp = null,
        scheduledTimestamp = "2026-01-01T00:00:02Z",
        initializedTimestamp = "2026-01-01T00:00:05Z",
        readyTimestamp = "2026-01-01T00:00:09Z",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = null,
          scheduledToInitialized = Duration.ofSeconds(3),
          initializedToReady = Duration.ofSeconds(4),
        ),
      )
  }

  @Test
  fun `does not record readiness when pod reached terminal state before becoming ready`() {
    val pod =
      pod(
        creationTimestamp = "2026-01-01T00:00:00Z",
        scheduledTimestamp = "2026-01-01T00:00:02Z",
        initializedTimestamp = "2026-01-01T00:00:05Z",
        readyTimestamp = "2026-01-01T00:00:09Z",
        readyStatus = "False",
      )

    assertThat(extractPodStartupDurations(pod))
      .isEqualTo(
        PodStartupDurations(
          createToScheduled = Duration.ofSeconds(2),
          scheduledToInitialized = Duration.ofSeconds(3),
          initializedToReady = null,
        ),
      )
  }

  @Test
  fun `strips version tag from connector image`() {
    assertThat(extractConnectorImage(podWithContainer("airbyte/source-postgres:3.6.1")))
      .isEqualTo("airbyte/source-postgres")
  }

  @Test
  fun `strips digest from connector image`() {
    assertThat(extractConnectorImage(podWithContainer("airbyte/source-postgres@sha256:abcdef")))
      .isEqualTo("airbyte/source-postgres")
  }

  @Test
  fun `preserves registry port when stripping connector image version`() {
    assertThat(extractConnectorImage(podWithContainer("registry.internal:5000/airbyte/source-postgres:3.6.1")))
      .isEqualTo("registry.internal:5000/airbyte/source-postgres")
  }

  @Test
  fun `returns unknown when main container or image is missing`() {
    assertThat(extractConnectorImage(podWithContainer("airbyte/source-postgres:3.6.1", "sidecar")))
      .isEqualTo("unknown")
    assertThat(extractConnectorImage(podWithContainer(null)))
      .isEqualTo("unknown")
  }

  @Test
  fun `returns unknown for an unparseable connector image`() {
    assertThat(extractConnectorImage(podWithContainer(" ")))
      .isEqualTo("unknown")
    assertThat(extractConnectorImage(podWithContainer("airbyte/source-postgres:")))
      .isEqualTo("unknown")
  }

  private fun pod(
    creationTimestamp: String?,
    scheduledTimestamp: String? = null,
    initializedTimestamp: String? = null,
    readyTimestamp: String? = null,
    readyStatus: String = "True",
  ): Pod =
    Pod().apply {
      metadata = ObjectMetaBuilder().withCreationTimestamp(creationTimestamp).build()
      status =
        PodStatusBuilder()
          .withConditions(
            listOfNotNull(
              scheduledTimestamp?.let { condition("PodScheduled", it) },
              initializedTimestamp?.let { condition("Initialized", it) },
              readyTimestamp?.let { condition("Ready", it, readyStatus) },
            ),
          ).build()
    }

  private fun podWithContainer(
    image: String?,
    containerName: String = ContainerConstants.MAIN_CONTAINER_NAME,
  ): Pod =
    Pod().apply {
      spec =
        PodSpecBuilder()
          .withContainers(
            listOf(
              ContainerBuilder()
                .withName(containerName)
                .build()
                .apply { this.image = image },
            ),
          ).build()
    }

  private fun condition(
    type: String,
    timestamp: String,
    status: String = "True",
  ) = PodConditionBuilder()
    .withType(type)
    .withStatus(status)
    .withLastTransitionTime(timestamp)
    .build()
}
