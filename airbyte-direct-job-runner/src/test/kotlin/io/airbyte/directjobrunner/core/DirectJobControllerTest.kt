/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.core

import io.airbyte.config.WorkloadType
import io.airbyte.workload.api.domain.WorkloadStatus
import io.micronaut.http.HttpStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DirectJobControllerTest {
  private val service = mockk<DirectJobService>()
  private val controller = DirectJobController(service)

  @Test
  fun `accepted returns 202`() {
    every { service.submit("w1") } returns DirectJobSubmission.Accepted

    assertEquals(HttpStatus.ACCEPTED, controller.submit(DirectJobRequest("w1")).status)
  }

  @Test
  fun `duplicate returns 409`() {
    every { service.submit("w1") } returns DirectJobSubmission.Duplicate

    assertEquals(HttpStatus.CONFLICT, controller.submit(DirectJobRequest("w1")).status)
  }

  @Test
  fun `ineligible workload returns 409`() {
    every { service.submit("w1") } returns DirectJobSubmission.NotEligible(WorkloadStatus.RUNNING)

    assertEquals(HttpStatus.CONFLICT, controller.submit(DirectJobRequest("w1")).status)
  }

  @Test
  fun `unsupported workload type returns 400`() {
    every { service.submit("w1") } returns DirectJobSubmission.Unsupported(WorkloadType.SYNC)

    assertEquals(HttpStatus.BAD_REQUEST, controller.submit(DirectJobRequest("w1")).status)
  }

  @Test
  fun `blank workload id returns 400 without calling the service`() {
    assertEquals(HttpStatus.BAD_REQUEST, controller.submit(DirectJobRequest("  ")).status)

    verify(exactly = 0) { service.submit(any()) }
  }
}
