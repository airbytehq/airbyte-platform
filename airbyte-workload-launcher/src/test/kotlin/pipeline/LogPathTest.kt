/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.pipeline.stages.BuildInputStage
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
import io.airbyte.workload.launcher.pipeline.stages.ClaimStage
import io.airbyte.workload.launcher.pipeline.stages.EnforceMutexStage
import io.airbyte.workload.launcher.pipeline.stages.LaunchPodStage
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.github.oshai.kotlinlogging.KotlinLogging
import io.mockk.every
import io.mockk.mockk
import java.io.File
import org.junit.jupiter.api.Test
import pipeline.SharedMocks.Companion.metricPublisher
import reactor.kotlin.core.publisher.toMono

private val LOGGER = KotlinLogging.logger {}

class LogPathTest {
  @Test
  fun `should write stage and success logs to file`() {
    val workloadId = "1"
    val dataplaneId = "dataplane_id"

    val logFile: File = File.createTempFile("log-path-1", ".txt")
    val launcherInputMessage =
      LauncherInput(workloadId, "workload-input", mapOf(), logFile.absolutePath)
    val launchStageIO = LaunchStageIO(launcherInputMessage)

    val claim: ClaimStage = claimStage(launchStageIO)
    val check: CheckStatusStage = checkStatusStage(launchStageIO)
    val build: BuildInputStage = buildInputStage(launchStageIO)
    val mutex: EnforceMutexStage = mutexStage(launchStageIO)
    val launch: LaunchPodStage = launchPodStage(launchStageIO, false)
    val statusUpdater: StatusUpdater = mockk()

    val launchPipeline =
      LaunchPipeline(dataplaneId, claim, check, build, mutex, launch, statusUpdater, metricPublisher)
    launchPipeline.accept(launcherInputMessage)

    assertLogFileContent(logFile, false)
  }

  @Test
  fun `should write stage and failure logs to file`() {
    val workloadId = "1"
    val dataplaneId = "dataplane_id"

    val logFile: File = File.createTempFile("log-path-1", ".txt")
    val launcherInputMessage =
      LauncherInput(workloadId, "workload-input", mapOf(), logFile.absolutePath)
    val launchStageIO = LaunchStageIO(launcherInputMessage)

    val claim: ClaimStage = claimStage(launchStageIO)
    val check: CheckStatusStage = checkStatusStage(launchStageIO)
    val build: BuildInputStage = buildInputStage(launchStageIO)
    val mutex: EnforceMutexStage = mutexStage(launchStageIO)
    val launch: LaunchPodStage = launchPodStage(launchStageIO, true)
    val statusUpdater: StatusUpdater =
      mockk {
        every {
          reportFailure(any())
        } answers {
          LOGGER.info { "Failure for workload " + launchStageIO.msg.workloadId }
        }
      }
    val launchPipeline: LaunchPipeline =
      LaunchPipeline(dataplaneId, claim, check, build, mutex, launch, statusUpdater, metricPublisher)
    launchPipeline.accept(launcherInputMessage)

    assertLogFileContent(logFile, true)
  }

  private fun buildInputStage(launchStageIO: LaunchStageIO): BuildInputStage {
    val build: BuildInputStage =
      mockk {
        every {
          apply(launchStageIO)
        } answers {
          LOGGER.info { StageName.BUILD.name + " for workload " + launchStageIO.msg.workloadId }
          launchStageIO.toMono()
        }
      }
    return build
  }

  private fun checkStatusStage(launchStageIO: LaunchStageIO): CheckStatusStage {
    val check: CheckStatusStage =
      mockk {
        every {
          apply(launchStageIO)
        } answers {
          LOGGER.info { StageName.CHECK_STATUS.name + " for workload " + launchStageIO.msg.workloadId }
          launchStageIO.toMono()
        }
      }
    return check
  }

  private fun claimStage(launchStageIO: LaunchStageIO): ClaimStage {
    val claim: ClaimStage =
      mockk {
        every {
          apply(launchStageIO)
        } answers {
          LOGGER.info { StageName.CLAIM.name + " for workload " + launchStageIO.msg.workloadId }
          launchStageIO.toMono()
        }
      }
    return claim
  }

  private fun mutexStage(launchStageIO: LaunchStageIO): EnforceMutexStage {
    val mutex: EnforceMutexStage =
      mockk {
        every {
          apply(launchStageIO)
        } answers {
          LOGGER.info { StageName.MUTEX.name + " for workload " + launchStageIO.msg.workloadId }
          launchStageIO.toMono()
        }
      }
    return mutex
  }

  private fun launchPodStage(
    launchStageIO: LaunchStageIO,
    throwException: Boolean,
  ): LaunchPodStage {
    val launch: LaunchPodStage =
      mockk {
        every {
          apply(launchStageIO)
        } answers {
          LOGGER.info { StageName.LAUNCH.name + " for workload " + launchStageIO.msg.workloadId }
          if (throwException) {
            throw StageError(
              launchStageIO,
              StageName.LAUNCH,
              RuntimeException("exception occurred"),
            )
          }
          launchStageIO.toMono()
        }
      }
    return launch
  }

  private fun assertLogFileContent(
    logFile: File,
    assertException: Boolean,
  ) {
    val fileContent = StringBuilder()
    logFile.forEachLine { c ->
      fileContent.append(c)
    }
    val completeFileContent = fileContent.toString()

    assert(completeFileContent.isNotBlank()) { "File content was blank" }
    assert(completeFileContent.contains("CLAIM for workload 1")) { "CLAIM line missing" }
    assert(completeFileContent.contains("CHECK_STATUS for workload 1")) { "CHECK_STATUS line missing" }
    assert(completeFileContent.contains("BUILD for workload 1")) { "BUILD line missing" }
    assert(completeFileContent.contains("LAUNCH for workload 1")) { "LAUNCH line missing" }
    if (assertException) {
      assert(completeFileContent.contains("Failure for workload 1")) { "FAILURE line missing" }
      assert(completeFileContent.contains("Completed without launching workload")) { "TERMINAL line missing" }
    } else {
      assert(completeFileContent.contains("Success: ")) { "SUCCESS line missing" }
    }
  }
}
