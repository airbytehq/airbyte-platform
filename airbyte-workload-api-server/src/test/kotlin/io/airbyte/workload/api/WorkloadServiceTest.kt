/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.DataplaneApi
import io.airbyte.api.client.model.generated.DataplaneReadId
import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
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

class WorkloadServiceTest {
  private val messageProducer: TemporalMessageProducer<LauncherInputMessage> = mockk()
  private val metricClient: MetricClient = mockk()
  private val airbyteApiClient: AirbyteApiClient = mockk()
  private val dataplaneApi: DataplaneApi = mockk()

  private val workloadId = "workloadIdea"
  private val workloadInput = "{}"
  private val labels = mapOf("la" to "bel")
  private val logPath = "log/path"
  private val mutexKey = "mutex"
  private val autoId = UUID.randomUUID()
  private val dataplaneId = "dataplaneId"

  @BeforeEach
  fun init() {
    clearAllMocks()
    every { messageProducer.publish(any(), any(), any()) } returns Unit
    every { metricClient.count(metric = any(), value = any(), attributes = anyVararg<MetricAttribute>()) } returns mockk<Counter>()
    every { airbyteApiClient.dataplaneApi } returns dataplaneApi
    every { dataplaneApi.getDataplaneId(any()) } returns DataplaneReadId(dataplaneId)
  }

  @ParameterizedTest
  @MethodSource("expectedQueueArgsMatrix")
  fun `Use the right queue`(
    workloadType: WorkloadType,
    priority: WorkloadPriority,
    expectedQueue: String,
  ) {
    val workloadService = WorkloadService(messageProducer, metricClient, airbyteApiClient)

    workloadService.create(workloadId, workloadInput, labels, logPath, mutexKey, workloadType, autoId, priority)

    verify { messageProducer.publish(eq(expectedQueue), any(), eq("wl-create_$workloadId")) }
  }

  companion object {
    private const val REGULAR_QUEUE = "regularQueue"
    private const val HIGH_PRIORITY_QUEUE = "highPriorityQueue"
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
