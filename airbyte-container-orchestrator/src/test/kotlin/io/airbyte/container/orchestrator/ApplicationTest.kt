/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.container.orchestrator.worker.ReplicationJobOrchestrator
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.util.Optional

internal class ApplicationTest {
  private lateinit var jobOrchestrator: ReplicationJobOrchestrator
  private lateinit var replicationLogMdcBuilder: MdcScope.Builder
  private lateinit var flexLogAppenderInitializer: FlexLogAppenderInitializer

  @BeforeEach
  fun setup() {
    jobOrchestrator = mockk()
    replicationLogMdcBuilder = MdcScope.Builder().setExtraMdcEntriesNonNullable(LogSource.REPLICATION_ORCHESTRATOR.toMdc())
    flexLogAppenderInitializer = mockk(relaxed = true)
  }

  @Test
  fun testHappyPath() {
    every { jobOrchestrator.runJob() } returns Optional.of("result output")
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)
    val code = app.run()

    assertEquals(SUCCESS_EXIT_CODE, code)
    verify(exactly = 1) { jobOrchestrator.runJob() }
  }

  @Test
  fun testJobFailedWritesFailedStatus() {
    every { jobOrchestrator.runJob() } throws Exception()
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)
    val code = app.run()

    assertEquals(FAILURE_EXIT_CODE, code)
    verify(exactly = 1) { jobOrchestrator.runJob() }
  }

  @Test
  fun `initializes flex logging once before replication MDC and job execution`() {
    val mdcKey =
      LogSource.REPLICATION_ORCHESTRATOR
        .toMdc()
        .keys
        .single()
    every { flexLogAppenderInitializer.initialize() } answers { assertEquals(null, MDC.get(mdcKey)) }
    every { jobOrchestrator.runJob() } answers {
      assertEquals(LogSource.REPLICATION_ORCHESTRATOR.displayName, MDC.get(mdcKey))
      Optional.of("result output")
    }
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)

    assertEquals(SUCCESS_EXIT_CODE, app.run())

    verify(exactly = 1) { flexLogAppenderInitializer.initialize() }
    verify(exactly = 1) { jobOrchestrator.runJob() }
  }

  @Test
  fun `virtual machine error from flex initialization propagates without running the job`() {
    val failure = OutOfMemoryError("fatal initialization")
    every { flexLogAppenderInitializer.initialize() } throws failure
    every { jobOrchestrator.runJob() } returns Optional.of("must not run")
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)

    assertSame(failure, assertThrows(OutOfMemoryError::class.java) { app.run() })
    verify(exactly = 0) { jobOrchestrator.runJob() }
  }

  @Test
  fun `interrupted flex initialization remains nonfatal and restores interrupt status`() {
    every { flexLogAppenderInitializer.initialize() } throws InterruptedException("interrupted initialization")
    every { jobOrchestrator.runJob() } returns Optional.of("result output")
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)

    try {
      assertDoesNotThrow { assertEquals(SUCCESS_EXIT_CODE, app.run()) }
      assertTrue(Thread.currentThread().isInterrupted)
      verify(exactly = 1) { jobOrchestrator.runJob() }
    } finally {
      Thread.interrupted()
    }
  }

  @Test
  fun `virtual machine error from job execution propagates`() {
    val failure = OutOfMemoryError("fatal job")
    every { jobOrchestrator.runJob() } throws failure
    val app = Application(jobOrchestrator, replicationLogMdcBuilder, flexLogAppenderInitializer)

    assertSame(failure, assertThrows(OutOfMemoryError::class.java) { app.run() })
  }

  @Test
  fun `entry point propagates virtual machine error without diagnostics or exit`() {
    val failure = OutOfMemoryError("credential-bearing fatal message")
    val diagnostics = mutableListOf<String>()
    val exitCodes = mutableListOf<Int>()

    val thrown =
      assertThrows(OutOfMemoryError::class.java) {
        runApplication(
          args = emptyArray(),
          startApplication = { throw failure },
          reportFailure = diagnostics::add,
          exit = exitCodes::add,
        )
      }

    assertSame(failure, thrown)
    assertTrue(diagnostics.isEmpty())
    assertTrue(exitCodes.isEmpty())
  }

  @Test
  fun `entry point sanitizes recoverable startup failure and exits with failure`() {
    val diagnostics = mutableListOf<String>()
    val exitCodes = mutableListOf<Int>()

    assertDoesNotThrow {
      runApplication(
        args = emptyArray(),
        startApplication = { throw IllegalStateException("secret token") },
        reportFailure = diagnostics::add,
        exit = exitCodes::add,
      )
    }

    assertEquals(listOf("Could not run container orchestrator."), diagnostics)
    assertEquals(listOf(FAILURE_EXIT_CODE), exitCodes)
    assertTrue(diagnostics.none { it.contains("secret token") })
  }

  @Test
  fun `entry point contains interrupted diagnostic failure and exits with failure`() {
    val exitCodes = mutableListOf<Int>()

    try {
      assertDoesNotThrow {
        runApplication(
          args = emptyArray(),
          startApplication = { throw IllegalStateException("startup failure") },
          reportFailure = { throw InterruptedException("diagnostic failure") },
          exit = exitCodes::add,
        )
      }

      assertTrue(Thread.currentThread().isInterrupted)
      assertEquals(listOf(FAILURE_EXIT_CODE), exitCodes)
    } finally {
      Thread.interrupted()
    }
  }
}
