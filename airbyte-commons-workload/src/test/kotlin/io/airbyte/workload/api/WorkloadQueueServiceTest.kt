/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.common.WorkloadQueueService
import io.airbyte.workload.repository.WorkloadQueueRepository
import io.airbyte.workload.repository.domain.WorkloadQueueItem
import io.micrometer.core.instrument.Counter
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

class WorkloadQueueServiceTest {
  private val metricClient: MetricClient = mockk()
  private val workloadQueueRepository: WorkloadQueueRepository = mockk()

  private val workloadId = "workloadIdea"
  private val workloadInput = "{}"
  private val labels = mapOf("la" to "bel")
  private val logPath = "log/path"
  private val mutexKey = "mutex"
  private val autoId = UUID.randomUUID()

  @BeforeEach
  fun init() {
    clearAllMocks()
    every { metricClient.count(metric = any(), value = any(), attributes = anyVararg<MetricAttribute>()) } returns mockk<Counter>()
  }

  @ParameterizedTest
  @MethodSource("expectedQueueArgsMatrix")
  fun `enqueues workload in workload queue table`(
    workloadType: WorkloadType,
    priority: WorkloadPriority,
    expectedQueue: String,
  ) {
    every { workloadQueueRepository.enqueueWorkload(expectedQueue, priority.toInt(), workloadId) } returns
      WorkloadQueueItem(
        dataplaneGroup = expectedQueue,
        priority = priority.toInt(),
        workloadId = workloadId,
        pollDeadline = null,
      )
    val workloadQueueService = WorkloadQueueService(metricClient, workloadQueueRepository)

    workloadQueueService.create(workloadId, workloadInput, labels, logPath, mutexKey, workloadType, autoId, priority, expectedQueue)

    verify { workloadQueueRepository.enqueueWorkload(expectedQueue, priority.toInt(), workloadId) }
  }

  companion object {
    private const val DATAPLANE_ID = "dataplaneId"

    @JvmStatic
    fun expectedQueueArgsMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(WorkloadType.SYNC, WorkloadPriority.DEFAULT, DATAPLANE_ID),
        Arguments.of(WorkloadType.CHECK, WorkloadPriority.HIGH, DATAPLANE_ID),
        Arguments.of(WorkloadType.CHECK, WorkloadPriority.DEFAULT, DATAPLANE_ID),
        Arguments.of(WorkloadType.DISCOVER, WorkloadPriority.HIGH, DATAPLANE_ID),
        Arguments.of(WorkloadType.DISCOVER, WorkloadPriority.DEFAULT, DATAPLANE_ID),
      )
  }
}
