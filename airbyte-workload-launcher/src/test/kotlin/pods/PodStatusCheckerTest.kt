/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pods

import io.fabric8.kubernetes.api.model.ContainerStateWaitingBuilder
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder
import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.api.model.PodStatusBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class PodStatusCheckerTest {
  @Test
  fun `checkForImagePullErrors detects ErrImagePull in init container`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withInitContainerStatuses(
              ContainerStatusBuilder()
                .withName("init-container")
                .withImage("airbyte/init:1.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ErrImagePull")
                    .withMessage("Failed to pull image \"airbyte/init:1.0.0\": rpc error")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertEquals(1, errors.size)
    assertEquals("init-container", errors[0].containerName)
    assertEquals(PodStatusChecker.ContainerType.INIT, errors[0].containerType)
    assertEquals("airbyte/init:1.0.0", errors[0].image)
    assertEquals("ErrImagePull", errors[0].reason)
    assertEquals("Failed to pull image \"airbyte/init:1.0.0\": rpc error", errors[0].message)
  }

  @Test
  fun `checkForImagePullErrors detects ImagePullBackOff in init container`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withInitContainerStatuses(
              ContainerStatusBuilder()
                .withName("init-container")
                .withImage("airbyte/source-test:5.2.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ImagePullBackOff")
                    .withMessage("Back-off pulling image \"airbyte/source-test:5.2.0\"")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertEquals(1, errors.size)
    assertEquals("ImagePullBackOff", errors[0].reason)
    assertEquals("airbyte/source-test:5.2.0", errors[0].image)
  }

  @Test
  fun `checkForImagePullErrors detects ErrImagePull in main container`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withContainerStatuses(
              ContainerStatusBuilder()
                .withName("main-container")
                .withImage("airbyte/destination-postgres:2.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ErrImagePull")
                    .withMessage("Failed to pull image")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertEquals(1, errors.size)
    assertEquals("main-container", errors[0].containerName)
    assertEquals(PodStatusChecker.ContainerType.MAIN, errors[0].containerType)
    assertEquals("airbyte/destination-postgres:2.0.0", errors[0].image)
  }

  @Test
  fun `checkForImagePullErrors detects ImagePullBackOff in main container`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withContainerStatuses(
              ContainerStatusBuilder()
                .withName("main-container")
                .withImage("airbyte/destination-snowflake:1.5.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ImagePullBackOff")
                    .withMessage("Back-off pulling image")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertEquals(1, errors.size)
    assertEquals("ImagePullBackOff", errors[0].reason)
  }

  @Test
  fun `checkForImagePullErrors returns empty list when pod is null`() {
    val errors = PodStatusChecker.checkForImagePullErrors(null)

    assertTrue(errors.isEmpty())
  }

  @Test
  fun `checkForImagePullErrors returns empty list when pod status is null`() {
    val pod = PodBuilder().build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertTrue(errors.isEmpty())
  }

  @Test
  fun `checkForImagePullErrors returns empty list when no image pull errors`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withContainerStatuses(
              ContainerStatusBuilder()
                .withName("main-container")
                .withImage("airbyte/source-test:1.0.0")
                .withNewState()
                .withNewRunning()
                .endRunning()
                .endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertTrue(errors.isEmpty())
  }

  @Test
  fun `checkForImagePullErrors ignores other waiting reasons`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withContainerStatuses(
              ContainerStatusBuilder()
                .withName("main-container")
                .withImage("airbyte/source-test:1.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("CrashLoopBackOff")
                    .withMessage("Container crashed")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertTrue(errors.isEmpty())
  }

  @Test
  fun `checkForImagePullErrors handles multiple errors in both init and main containers`() {
    val pod =
      PodBuilder()
        .withStatus(
          PodStatusBuilder()
            .withInitContainerStatuses(
              ContainerStatusBuilder()
                .withName("init-1")
                .withImage("airbyte/init:1.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ErrImagePull")
                    .withMessage("Failed to pull init image")
                    .build(),
                ).endState()
                .build(),
              ContainerStatusBuilder()
                .withName("init-2")
                .withImage("airbyte/init:2.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ImagePullBackOff")
                    .withMessage("Back-off pulling init image")
                    .build(),
                ).endState()
                .build(),
            ).withContainerStatuses(
              ContainerStatusBuilder()
                .withName("main")
                .withImage("airbyte/source:1.0.0")
                .withNewState()
                .withWaiting(
                  ContainerStateWaitingBuilder()
                    .withReason("ErrImagePull")
                    .withMessage("Failed to pull main image")
                    .build(),
                ).endState()
                .build(),
            ).build(),
        ).build()

    val errors = PodStatusChecker.checkForImagePullErrors(pod)

    assertEquals(3, errors.size)
    assertEquals("init-1", errors[0].containerName)
    assertEquals(PodStatusChecker.ContainerType.INIT, errors[0].containerType)
    assertEquals("init-2", errors[1].containerName)
    assertEquals(PodStatusChecker.ContainerType.INIT, errors[1].containerType)
    assertEquals("main", errors[2].containerName)
    assertEquals(PodStatusChecker.ContainerType.MAIN, errors[2].containerType)
  }

  @Test
  fun `formatImagePullErrors formats single error with image name`() {
    val errors =
      listOf(
        PodStatusChecker.ImagePullError(
          containerName = "main-container",
          containerType = PodStatusChecker.ContainerType.MAIN,
          image = "airbyte/source-test:5.2.0",
          reason = "ImagePullBackOff",
          message = "Back-off pulling image",
        ),
      )

    val formatted = PodStatusChecker.formatImagePullErrors(errors)

    assertEquals("Failed to pull container image=airbyte/source-test:5.2.0", formatted)
  }

  @Test
  fun `formatImagePullErrors formats error without image name using container name`() {
    val errors =
      listOf(
        PodStatusChecker.ImagePullError(
          containerName = "main-container",
          containerType = PodStatusChecker.ContainerType.MAIN,
          image = null,
          reason = "ErrImagePull",
          message = "Failed to pull image",
        ),
      )

    val formatted = PodStatusChecker.formatImagePullErrors(errors)

    assertEquals("Failed to pull container image for main-container", formatted)
  }

  @Test
  fun `formatImagePullErrors formats multiple errors with semicolon separator`() {
    val errors =
      listOf(
        PodStatusChecker.ImagePullError(
          containerName = "init",
          containerType = PodStatusChecker.ContainerType.INIT,
          image = "airbyte/init:1.0.0",
          reason = "ErrImagePull",
          message = null,
        ),
        PodStatusChecker.ImagePullError(
          containerName = "main",
          containerType = PodStatusChecker.ContainerType.MAIN,
          image = "airbyte/source-test:2.0.0",
          reason = "ImagePullBackOff",
          message = null,
        ),
      )

    val formatted = PodStatusChecker.formatImagePullErrors(errors)

    assertEquals("Failed to pull container image=airbyte/init:1.0.0; Failed to pull container image=airbyte/source-test:2.0.0", formatted)
  }
}
