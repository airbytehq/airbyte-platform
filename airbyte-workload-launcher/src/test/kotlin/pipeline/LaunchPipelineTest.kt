/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package pipeline

import fixtures.RecordFixtures.launcherInput
import io.airbyte.workload.launcher.pipeline.LaunchPipeline
import io.airbyte.workload.launcher.pipeline.PipelineIngressAdapter
import io.airbyte.workload.launcher.pipeline.handlers.FailureHandler
import io.airbyte.workload.launcher.pipeline.handlers.SuccessHandler
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.lang.RuntimeException

@ExtendWith(MockKExtension::class)
class LaunchPipelineTest {
  @MockK(relaxed = true)
  private lateinit var successHandler: SuccessHandler

  @MockK(relaxed = true)
  private lateinit var failureHandler: FailureHandler

  @MockK(relaxed = true)
  private lateinit var ingressAdapter: PipelineIngressAdapter

  private lateinit var pipeline: LaunchPipeline

  @BeforeEach
  fun setup() {
    every { ingressAdapter.apply(any()) } answers { LaunchStageIO(msg = firstArg()) }

    pipeline =
      LaunchPipeline(
        build = MockStage(StageName.BUILD),
        claim = MockStage(StageName.CLAIM),
        loadShed = MockStage(StageName.LOAD_SHED),
        check = MockStage(StageName.CHECK_STATUS),
        mutex = MockStage(StageName.MUTEX),
        architecture = MockStage(StageName.ARCHITECTURE),
        launch = MockStage(StageName.LAUNCH),
        successHandler = successHandler,
        failureHandler = failureHandler,
        ingressAdapter = ingressAdapter,
      )
  }

  @Test
  fun `the pipeline processes incoming messages`() {
    val workload1 = launcherInput("workload-1")
    val workload2 = launcherInput("workload-2")
    val workload3 = launcherInput("workload-3")

    val inputFlux =
      Flux.just(
        workload1,
        workload2,
        workload3,
      )

    val appliedPipe = pipeline.apply(inputFlux)

    StepVerifier
      .create(appliedPipe)
      .expectNext(LaunchStageIO(msg = workload1))
      .expectNext(LaunchStageIO(msg = workload2))
      .expectNext(LaunchStageIO(msg = workload3))
      .verifyComplete()

    verify { successHandler.accept(LaunchStageIO(msg = workload1)) }
    verify { successHandler.accept(LaunchStageIO(msg = workload2)) }
    verify { successHandler.accept(LaunchStageIO(msg = workload3)) }
  }

  @Test
  fun `the pipeline handles stage errors and continues processing incoming messages`() {
    val workload1 = launcherInput("workload-1")
    val workload2 = launcherInput("workload-2")
    val workload3 = launcherInput("workload-3")

    val inputFlux =
      Flux.just(
        workload1,
        workload2,
        workload3,
      )

    val exception = RuntimeException("kube timeout")

    pipeline =
      LaunchPipeline(
        build = MockStage(StageName.BUILD),
        claim = MockStage(StageName.CLAIM),
        loadShed = MockStage(StageName.LOAD_SHED),
        // simulate workload-2 having an error
        check = MockStage(StageName.CHECK_STATUS) { if (it.msg == workload2) throw exception else it },
        mutex = MockStage(StageName.MUTEX),
        architecture = MockStage(StageName.ARCHITECTURE),
        launch = MockStage(StageName.LAUNCH),
        successHandler = successHandler,
        failureHandler = failureHandler,
        ingressAdapter = ingressAdapter,
      )

    val appliedPipe = pipeline.apply(inputFlux)

    StepVerifier
      .create(appliedPipe)
      .expectNext(LaunchStageIO(msg = workload1))
      .expectNext(LaunchStageIO(msg = workload3))
      .verifyComplete()

    verify { successHandler.accept(LaunchStageIO(msg = workload1)) }
    verify { failureHandler.accept(any(), LaunchStageIO(msg = workload2)) }
    verify { successHandler.accept(LaunchStageIO(msg = workload3)) }
  }

  // Manually mock as using mockk w/ inheritance and Reactor gets complicated
  class MockStage(
    val name: StageName,
    val callback: (LaunchStageIO) -> LaunchStageIO = { it },
  ) : LaunchStage(mockk(relaxed = true)) {
    override fun applyStage(input: LaunchStageIO): LaunchStageIO = callback(input)

    override fun getStageName(): StageName = name
  }
}
