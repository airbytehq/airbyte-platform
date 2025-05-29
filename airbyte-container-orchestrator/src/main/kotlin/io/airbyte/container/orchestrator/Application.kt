/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.logging.MdcScope
import io.airbyte.container.orchestrator.worker.ReplicationJobOrchestrator
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.runtime.Micronaut.build
import jakarta.inject.Named
import jakarta.inject.Singleton
import kotlin.system.exitProcess

private val logger = KotlinLogging.logger {}

internal const val FAILURE_EXIT_CODE = 1
internal const val SUCCESS_EXIT_CODE = 0

fun main(args: Array<String>) {
  // To mimic previous behavior, assume an exit code of 1 unless Application.run returns otherwise.
  var exitCode = FAILURE_EXIT_CODE
  try {
    build(*args)
      .deduceCloudEnvironment(false)
      .deduceEnvironment(false)
      .mainClass(Application::class.java)
      .start()
      .use { ctx ->
        exitCode = ctx.getBean(Application::class.java).run()
      }
  } catch (t: Throwable) {
    logger.error(t) { "could not run ${t.message}" }
  } finally {
    // this mimics the pre-micronaut code, unsure if there is a better way in micronaut to ensure a
    // non-zero exit code
    exitProcess(status = exitCode)
  }
}

@SuppressWarnings("PMD.AvoidCatchingThrowable", "PMD.DoNotTerminateVM", "PMD.AvoidFieldNameMatchingTypeName", "PMD.UnusedLocalVariable")
@Singleton
class Application(
  private val jobOrchestrator: ReplicationJobOrchestrator,
  @Named("replicationMdcScopeBuilder") private val replicationLogMdcBuilder: MdcScope.Builder,
) {
  /**
   * Configures logging/mdc scope, and creates all objects necessary to handle state updates.
   *
   *
   * Handles state updates (including writing failures) and running the job orchestrator. As much of
   * the initialization as possible should go in here, so it's logged properly and the state storage
   * is updated appropriately.
   */
  @VisibleForTesting
  fun run(): Int =
    // set mdc scope for the remaining execution
    replicationLogMdcBuilder.build().use { _ ->
      try {
        val result: String = jobOrchestrator.runJob().orElse("")
        logger.debug { "Job orchestrator completed with result: $result" }
        SUCCESS_EXIT_CODE
      } catch (t: Throwable) {
        logger.error(t) { "Killing orchestrator because of an Exception" }
        FAILURE_EXIT_CODE
      }
    }
}
