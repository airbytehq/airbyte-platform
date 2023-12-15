/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline

import fixtures.RecordFixtures
import io.airbyte.config.Configs
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.launcher.ClaimedProcessor
import io.airbyte.workload.launcher.client.LogContextFactory
import io.airbyte.workload.launcher.client.WorkloadApiClient
import io.airbyte.workload.launcher.fixtures.SharedMocks.Companion.metricPublisher
import io.airbyte.workload.launcher.fixtures.TestStage
import io.airbyte.workload.launcher.pipeline.LogPathTest.Fixtures.inputMsgs
import io.airbyte.workload.launcher.pipeline.LogPathTest.Fixtures.launchPipeline
import io.airbyte.workload.launcher.pipeline.LogPathTest.Fixtures.readTestLogs
import io.airbyte.workload.launcher.pipeline.consumer.LauncherInput
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandler
import io.airbyte.workload.launcher.pipeline.handlers.SuccessHandler
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.io.File
import java.nio.file.Files
import java.util.Optional
import java.util.function.Function
import java.util.stream.Stream
import kotlin.io.path.Path

class LogPathTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `singled-threaded queue-fed pipeline writes stage and completion logs to log path on input`(testErrorCase: Boolean) {
    val pipeline = launchPipeline(testErrorCase)

    val msgs = inputMsgs()

    msgs.parallelStream().forEach { msg ->
      pipeline.accept(msg)
    }

    msgs.forEach { msg ->
      val logLines = readTestLogs(msg.logPath)
      assert(logLines.isNotEmpty())
      assert(logLines[0].endsWith("TEST: Stage: CLAIM. Id: ${msg.workloadId}."))
      assert(logLines[1].endsWith("TEST: Stage: CHECK_STATUS. Id: ${msg.workloadId}."))
      assert(logLines[2].endsWith("TEST: Stage: BUILD. Id: ${msg.workloadId}."))
      assert(logLines[3].endsWith("TEST: Stage: MUTEX. Id: ${msg.workloadId}."))
      assert(logLines[4].endsWith("TEST: Stage: LAUNCH. Id: ${msg.workloadId}."))
      if (testErrorCase) {
        assert(logLines[5].endsWith("TEST: failure. Id: ${msg.workloadId}."))
      } else {
        assert(logLines[5].endsWith("TEST: success. Id: ${msg.workloadId}."))
      }
    }
  }

  @ParameterizedTest
  @MethodSource("processClaimedMatrix")
  fun `multi-threaded claimed processor (rehydrator) writes stage and completion logs to log path on input`(
    testErrorCase: Boolean,
    parallelism: Int,
  ) {
    val pipeline = launchPipeline(testErrorCase)

    val processor =
      ClaimedProcessor(
        mockk<WorkloadApi>(),
        pipeline,
        metricPublisher,
        "dataplane_id",
        parallelism,
      )

    val msgs = inputMsgs()

    processor.processMessages(msgs)

    msgs.forEach { msg ->
      val logLines = readTestLogs(msg.logPath)
      assert(logLines.isNotEmpty())
      assert(logLines[0].endsWith("TEST: Stage: CLAIM. Id: ${msg.workloadId}."))
      assert(logLines[1].endsWith("TEST: Stage: CHECK_STATUS. Id: ${msg.workloadId}."))
      assert(logLines[2].endsWith("TEST: Stage: BUILD. Id: ${msg.workloadId}."))
      assert(logLines[3].endsWith("TEST: Stage: MUTEX. Id: ${msg.workloadId}."))
      assert(logLines[4].endsWith("TEST: Stage: LAUNCH. Id: ${msg.workloadId}."))
      if (testErrorCase) {
        assert(logLines[5].endsWith("TEST: failure. Id: ${msg.workloadId}."))
      } else {
        assert(logLines[5].endsWith("TEST: success. Id: ${msg.workloadId}."))
      }
    }
  }

  companion object {
    @JvmStatic
    fun processClaimedMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(false, 4),
        Arguments.of(false, 1),
        Arguments.of(false, 5),
        Arguments.of(true, 4),
        Arguments.of(true, 1),
        Arguments.of(true, 6),
      )
    }
  }

  object Fixtures {
    private val mockApiClient: WorkloadApiClient =
      mockk {
        every { reportFailure(any()) } returns Unit
      }
    private val successHandler = SuccessHandler(mockApiClient, metricPublisher, Optional.of(Function { id -> "TEST: success. Id: $id." }))
    private val failureHandler = FailureHandler(mockApiClient, metricPublisher, Optional.of(Function { id -> "TEST: failure. Id: $id." }))

    private const val TEST_LOG_PREFIX = "TEST"

    private val stageLogMsgFn = { name: StageName, io: LaunchStageIO -> "$TEST_LOG_PREFIX: Stage: $name. Id: ${io.msg.workloadId}." }

    fun inputMsgs(): List<LauncherInput> {
      val logFile1: File = File.createTempFile("log-path-1", ".txt")
      val logFile2: File = File.createTempFile("log-path-2", ".txt")
      val logFile3: File = File.createTempFile("log-path-3", ".txt")
      val logFile4: File = File.createTempFile("log-path-4", ".txt")
      val logFile5: File = File.createTempFile("log-path-5", ".txt")
      return listOf(
        RecordFixtures.launcherInput(workloadId = "1", logPath = logFile1.absolutePath),
        RecordFixtures.launcherInput(workloadId = "2", logPath = logFile2.absolutePath),
        RecordFixtures.launcherInput(workloadId = "3", logPath = logFile3.absolutePath),
        RecordFixtures.launcherInput(workloadId = "4", logPath = logFile4.absolutePath),
        RecordFixtures.launcherInput(workloadId = "5", logPath = logFile5.absolutePath),
      )
    }

    fun launchPipeline(testErrorCase: Boolean) =
      LaunchPipeline(
        "dataplane_id",
        TestStage(StageName.CLAIM, stageLogMsgFn),
        TestStage(StageName.CHECK_STATUS, stageLogMsgFn),
        TestStage(StageName.BUILD, stageLogMsgFn),
        TestStage(StageName.MUTEX, stageLogMsgFn),
        TestStage(StageName.LAUNCH, stageLogMsgFn, testErrorCase),
        successHandler,
        failureHandler,
        metricPublisher,
        LogContextFactory(Configs.WorkerEnvironment.DOCKER),
      )

    fun readTestLogs(logPath: String): List<String> = Files.readAllLines(Path(logPath)).filter { line -> line.contains(TEST_LOG_PREFIX) }
  }
}
