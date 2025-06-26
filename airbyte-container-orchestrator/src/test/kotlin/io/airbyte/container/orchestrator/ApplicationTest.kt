/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import java.util.Optional

internal class ApplicationTest {
  private lateinit var jobOrchestrator: ReplicationJobOrchestrator
  private lateinit var replicationLogMdcBuilder: MdcScope.Builder

  @BeforeEach
  fun setup() {
    jobOrchestrator = mockk()
    replicationLogMdcBuilder = MdcScope.Builder().setExtraMdcEntries(LogSource.REPLICATION_ORCHESTRATOR.toMdc())
  }

  @Test
  @Throws(Exception::class)
  fun testHappyPath() {
    every { jobOrchestrator.runJob() } returns Optional.of("result output")
    val app = Application(jobOrchestrator = jobOrchestrator, replicationLogMdcBuilder = replicationLogMdcBuilder)
    val code = app.run()

    assertEquals(SUCCESS_EXIT_CODE, code)
    verify(exactly = 1) { jobOrchestrator.runJob() }
  }

  @Test
  @Throws(Exception::class)
  fun testJobFailedWritesFailedStatus() {
    every { jobOrchestrator.runJob() } throws Exception()
    val app = Application(jobOrchestrator = jobOrchestrator, replicationLogMdcBuilder = replicationLogMdcBuilder)
    val code = app.run()

    assertEquals(FAILURE_EXIT_CODE, code)
    verify(exactly = 1) { jobOrchestrator.runJob() }
  }
}
