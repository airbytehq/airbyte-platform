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
import org.junit.jupiter.api.Assertions.assertEquals
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
}
