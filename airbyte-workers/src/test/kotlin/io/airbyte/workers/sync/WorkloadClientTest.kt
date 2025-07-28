/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync

import io.airbyte.commons.temporal.HeartbeatUtils
import io.airbyte.workers.sync.WorkloadClient.Companion.CANCELLATION_SOURCE_STR
import io.airbyte.workers.workload.WorkloadConstants.WORKLOAD_CANCELLED_BY_USER_REASON
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.client.WorkloadApiClient
import io.airbyte.workload.api.client.generated.WorkloadApi
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest
import io.airbyte.workload.api.client.model.generated.WorkloadPriority
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.temporal.activity.ActivityExecutionContext
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicReference

class WorkloadClientTest {
  private val apiClientWrapper: WorkloadApiClient = mockk()
  private val apiClient: WorkloadApi = mockk()
  private val outputWriter: WorkloadOutputWriter = mockk()

  private lateinit var client: WorkloadClient

  @BeforeEach
  fun setup() {
    every { apiClientWrapper.workloadApi } returns apiClient

    client = spyk(WorkloadClient(apiClientWrapper, outputWriter))
  }

  @Test
  fun `cancelWorkloadBestEffort attempts to cancel the workflow`() {
    val req = WorkloadCancelRequest("workloadId", "reason", "source")

    every { apiClient.workloadCancel(req) } returns Unit

    client.cancelWorkloadBestEffort(req)

    verify { apiClient.workloadCancel(req) }
  }

  @Test
  fun `cancelWorkloadBestEffort swallows exceptions`() {
    val req = WorkloadCancelRequest("workloadId", "reason", "source")

    every { apiClient.workloadCancel(req) } throws Exception("bang")

    assertDoesNotThrow {
      client.cancelWorkloadBestEffort(req)
    }

    verify { apiClient.workloadCancel(req) }
  }

  @Test
  fun `runWorkloadWithCancellationHeartbeat wraps workload creation and waiting in a heartbeating thread that cancels the workload on failure`() {
    val cancellationCallbackSlot = slot<AtomicReference<Runnable>>()
    val callableSlot = slot<Callable<Unit>>()

    mockkStatic(HeartbeatUtils::class)
    every { HeartbeatUtils.withBackgroundHeartbeat(capture(cancellationCallbackSlot), capture(callableSlot), any()) } returns Unit

    val createReq =
      WorkloadCreateRequest(
        workloadId = "workloadId",
        labels = ArrayList(),
        workloadInput = "",
        logPath = "",
        type = WorkloadType.CHECK,
        priority = WorkloadPriority.DEFAULT,
      )
    val checkFreqSecs = 10
    val executionContext: ActivityExecutionContext =
      mockk {
        every { heartbeat(null) } returns Unit
      }

    client.runWorkloadWithCancellationHeartbeat(createReq, checkFreqSecs, executionContext)
    // validate we call the wrapper
    verify { HeartbeatUtils.withBackgroundHeartbeat(any(), any<Callable<Unit>>(), executionContext) }
    // validate the wrapped cancellation callback executes the code we expect
    every { client.cancelWorkloadBestEffort(any()) } returns Unit

    cancellationCallbackSlot.captured.get().run()
    val expectedCancellationPayload = WorkloadCancelRequest(createReq.workloadId, WORKLOAD_CANCELLED_BY_USER_REASON, CANCELLATION_SOURCE_STR)

    verify { client.cancelWorkloadBestEffort(expectedCancellationPayload) }
    // validate the wrapped 'callable' callback executes the code we expect
    every { client.createWorkload(createReq) } returns Unit
    every { client.waitForWorkload(createReq.workloadId, checkFreqSecs) } returns Unit

    callableSlot.captured.call()

    verify { client.createWorkload(createReq) }
    verify { client.waitForWorkload(createReq.workloadId, checkFreqSecs) }
  }
}
