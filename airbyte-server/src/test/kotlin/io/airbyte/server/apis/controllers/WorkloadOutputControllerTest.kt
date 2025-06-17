/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.WorkloadOutputWriteRequest
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.storage.StorageClient
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadStatus
import io.airbyte.workload.repository.domain.WorkloadType
import io.airbyte.workload.services.WorkloadService
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.UUID

@ExtendWith(MockKExtension::class)
class WorkloadOutputControllerTest {
  @MockK
  private lateinit var storageClient: StorageClient

  private lateinit var controller: WorkloadOutputController
  private val roleResolver = mockk<RoleResolver>(relaxed = true)
  private val workloadService = mockk<WorkloadService>()
  private val orgId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    controller = WorkloadOutputController(storageClient, workloadService, roleResolver)

    every { storageClient.write(any(), any()) } returns Unit
    every { workloadService.getWorkload(any()) } returns
      Workload(
        id = "workload 1",
        dataplaneId = null,
        status = WorkloadStatus.PENDING,
        workloadLabels = null,
        inputPayload = "",
        workspaceId = null,
        organizationId = orgId,
        logPath = "logs",
        mutexKey = "mutex",
        type = WorkloadType.SPEC,
        signalInput = "signalInput",
      )
  }

  @ParameterizedTest
  @CsvSource("'workload id 1', 'output 1'", "'workload id 2', 'output 2'", "'random stuff', 'more random stuff'")
  fun `proxies input to storage client write`(
    workloadId: String,
    output: String,
  ) {
    val req =
      WorkloadOutputWriteRequest()
        .workloadId(workloadId)
        .output(output)

    controller.writeWorkloadOutput(req)

    verify(exactly = 1) { storageClient.write(workloadId, output) }
  }

  @Test
  fun `surfaces does not swallow upstream errors`() {
    val exception = RuntimeException("bang")
    every { storageClient.write(any(), any()) } throws exception
    val req =
      WorkloadOutputWriteRequest()
        .workloadId("")
        .output("")

    assertThrows<RuntimeException> { controller.writeWorkloadOutput(req) }
  }
}
