package io.airbyte.workload.launcher.pipeline

import io.airbyte.workload.launcher.client.StatusUpdater
import io.airbyte.workload.launcher.mocks.LauncherInputMessage
import io.airbyte.workload.launcher.pipeline.stages.BuildInputStage
import io.airbyte.workload.launcher.pipeline.stages.CheckStatusStage
import io.airbyte.workload.launcher.pipeline.stages.ClaimStage
import io.airbyte.workload.launcher.pipeline.stages.LaunchPodStage
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import java.util.function.Consumer

private val logger = KotlinLogging.logger {}

@Singleton
class LaunchPipeline(
  private val claim: ClaimStage,
  private val check: CheckStatusStage,
  private val build: BuildInputStage,
  private val launch: LaunchPodStage,
  private val statusUpdater: StatusUpdater,
) : Consumer<LauncherInputMessage> {
  // todo: This is for when we get to back pressure: if we want backpressure on the Mono,
  //  we will need to create a scheduler that is used by all Monos to ensure that we have
  //  a max concurrent capacity. See the Schedulers class for details. We can even define
  //  an executor service via Micronaut configuration and inject that to pass to the
  //  scheduler (see the fromExecutorService method on Schedulers).
  override fun accept(msg: LauncherInputMessage) {
    LaunchStageIO(msg)
      .toMono()
      .flatMap(claim)
      .flatMap(check)
      .flatMap(build)
      .flatMap(launch)
      .onErrorResume(this::handleError)
      .doOnSuccess { r -> logger.info { ("Success: $r") } }
      .subscribe()
  }

  private fun handleError(e: Throwable): Mono<LaunchStageIO> {
    logger.error(e) { ("Pipeline Error: $e") }
    if (e is StageError) {
      statusUpdater.reportFailure(e)
    }
    return Mono.empty()
  }
}
