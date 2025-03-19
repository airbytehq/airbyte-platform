/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.handlers

import io.airbyte.config.WorkloadType
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandlerTest.Fixtures.launcherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandlerTest.Fixtures.stageIO
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandlerTest.Fixtures.workloadId
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.StageError
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.Optional
import java.util.UUID

@ExtendWith(MockKExtension::class)
class FailureHandlerTest {
  @MockK(relaxed = true)
  private lateinit var workloadApiClient: WorkloadApiClient

  private lateinit var handler: FailureHandler

  @BeforeEach
  fun setup() {
    handler =
      FailureHandler(
        workloadApiClient,
        mockk(relaxed = true),
        Optional.empty(),
      )
  }

  @CsvSource(
    value = [
      "CHECK_STATUS",
      "BUILD",
      "LOAD_SHED",
      "MUTEX",
      "LAUNCH",
    ],
  )
  @ParameterizedTest
  fun `a workload failure is reported to the Workload API for post-CLAIM stage errors`(stageName: StageName) {
    val error =
      StageError(
        io = stageIO,
        stageName = stageName,
        cause = RuntimeException("bang"),
      )

    handler.accept(error, stageIO)

    verify(exactly = 1) { workloadApiClient.reportFailure(workloadId, any()) }
  }

  @Test
  fun `a workload failure is not reported to the Workload API for non-stage errors`() {
    val error = RuntimeException("bang")

    handler.accept(error, launcherInput)

    verify(exactly = 0) { workloadApiClient.reportFailure(workloadId, any()) }
  }

  @Test
  fun `a workload failure is not reported to the Workload API for the claim stage`() {
    val error =
      StageError(
        io = stageIO,
        stageName = StageName.CLAIM,
        cause = RuntimeException("bang"),
      )

    handler.accept(error, stageIO)

    verify(exactly = 0) { workloadApiClient.reportFailure(workloadId, any()) }
  }

  object Fixtures {
    val workloadId = "workload-id"
    val launcherInput =
      LauncherInput(
        workloadId = workloadId,
        workloadInput = "",
        labels = mapOf(),
        logPath = "",
        workloadType = WorkloadType.SYNC,
        mutexKey = "",
        autoId = UUID.randomUUID(),
      )
    val stageIO =
      LaunchStageIO(
        msg = launcherInput,
        logCtx = mapOf(),
      )
  }
}
