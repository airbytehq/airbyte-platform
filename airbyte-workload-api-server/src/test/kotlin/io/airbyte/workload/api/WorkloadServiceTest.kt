package io.airbyte.workload.api

import io.airbyte.commons.temporal.queue.TemporalMessageProducer
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.WorkloadType
import io.airbyte.config.messages.LauncherInputMessage
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Geography
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Priority
import io.airbyte.featureflag.WorkloadApiRouting
import io.airbyte.workload.metrics.CustomMetricPublisher
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
  private val metricPublisher: CustomMetricPublisher = mockk()
  private val featureFlagClient: FeatureFlagClient = mockk()

  private val workloadId = "workloadIdea"
  private val workloadInput = "{}"
  private val labels = mapOf("la" to "bel")
  private val logPath = "log/path"
  private val geography = "geo"
  private val mutexKey = "mutex"
  private val autoId = UUID.randomUUID()

  @BeforeEach
  fun init() {
    clearAllMocks()
    every { messageProducer.publish(any(), any(), any()) } returns Unit
    every { metricPublisher.count(any(), any(), any(), any()) } returns Unit
    every { featureFlagClient.stringVariation(WorkloadApiRouting, Multi(listOf(Geography(geography)))) } returns REGULAR_QUEUE
    every {
      featureFlagClient.stringVariation(WorkloadApiRouting, Multi(listOf(Geography(geography), Priority(Priority.HIGH_PRIORITY))))
    } returns HIGH_PRIORITY_QUEUE
  }

  @ParameterizedTest
  @MethodSource("expectedQueueArgsMatrix")
  fun `Use the right queue`(
    workloadType: WorkloadType,
    priority: WorkloadPriority,
    expectedQueue: String,
  ) {
    val workloadService = WorkloadService(messageProducer, metricPublisher, featureFlagClient)

    workloadService.create(workloadId, workloadInput, labels, logPath, geography, mutexKey, workloadType, autoId, priority)

    verify { messageProducer.publish(eq(expectedQueue), any(), eq("wl-create_$workloadId")) }
  }

  companion object {
    const val REGULAR_QUEUE = "regularQueue"
    const val HIGH_PRIORITY_QUEUE = "highPriorityQueue"

    @JvmStatic
    fun expectedQueueArgsMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(WorkloadType.SYNC, WorkloadPriority.DEFAULT, REGULAR_QUEUE),
        Arguments.of(WorkloadType.CHECK, WorkloadPriority.HIGH, HIGH_PRIORITY_QUEUE),
        Arguments.of(WorkloadType.CHECK, WorkloadPriority.DEFAULT, REGULAR_QUEUE),
        Arguments.of(WorkloadType.DISCOVER, WorkloadPriority.HIGH, HIGH_PRIORITY_QUEUE),
        Arguments.of(WorkloadType.DISCOVER, WorkloadPriority.DEFAULT, REGULAR_QUEUE),
      )
    }
  }
}
