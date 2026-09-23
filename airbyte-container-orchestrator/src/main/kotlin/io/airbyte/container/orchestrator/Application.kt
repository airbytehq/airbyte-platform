/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.commons.annotation.InternalForTesting
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
  runApplication(args)
}

@SuppressWarnings("PMD.DoNotTerminateVM")
internal fun runApplication(
  args: Array<String>,
  startApplication: (Array<String>) -> Int = { applicationArgs ->
    build(*applicationArgs)
      .deduceCloudEnvironment(false)
      .deduceEnvironment(false)
      .mainClass(Application::class.java)
      .start()
      .use { ctx ->
        ctx.getBean(Application::class.java).run()
      }
  },
  reportFailure: (String) -> Unit = { message -> logger.error { message } },
  exit: (Int) -> Unit = ::exitProcess,
) {
  val exitCode =
    try {
      startApplication(args)
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      if (failure is InterruptedException) Thread.currentThread().interrupt()
      try {
        reportFailure("Could not run container orchestrator.")
      } catch (reportingFailure: Throwable) {
        if (reportingFailure is VirtualMachineError) throw reportingFailure
        if (reportingFailure is InterruptedException) Thread.currentThread().interrupt()
      }
      FAILURE_EXIT_CODE
    }
  exit(exitCode)
}

@SuppressWarnings("PMD.AvoidCatchingThrowable", "PMD.DoNotTerminateVM", "PMD.AvoidFieldNameMatchingTypeName", "PMD.UnusedLocalVariable")
@Singleton
class Application(
  private val jobOrchestrator: ReplicationJobOrchestrator,
  @Named("replicationMdcScopeBuilder") private val replicationLogMdcBuilder: MdcScope.Builder,
  private val flexLogAppenderInitializer: FlexLogAppenderInitializer,
) {
  /**
   * Configures logging/mdc scope, and creates all objects necessary to handle state updates.
   *
   *
   * Handles state updates (including writing failures) and running the job orchestrator. As much of
   * the initialization as possible should go in here, so it's logged properly and the state storage
   * is updated appropriately.
   */
  @InternalForTesting
  fun run(): Int {
    try {
      flexLogAppenderInitializer.initialize()
    } catch (failure: Throwable) {
      if (failure is VirtualMachineError) throw failure
      if (failure is InterruptedException) Thread.currentThread().interrupt()
      logger.warn { "Unable to initialize workload log delivery. Continuing workload execution." }
    }

    // set mdc scope for the remaining execution
    return replicationLogMdcBuilder.build().use { _ ->
      try {
        val result: String = jobOrchestrator.runJob().orElse("")
        logger.debug { "Job orchestrator completed with result: $result" }
        SUCCESS_EXIT_CODE
      } catch (t: Throwable) {
        if (t is VirtualMachineError) throw t
        if (t is InterruptedException) Thread.currentThread().interrupt()
        logger.error(t) { "Killing orchestrator because of an Exception" }
        FAILURE_EXIT_CODE
      }
    }
  }
}
