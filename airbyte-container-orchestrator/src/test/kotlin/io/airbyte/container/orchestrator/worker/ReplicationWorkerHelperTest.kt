/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.MapperConfig
import io.airbyte.config.StandardSyncSummary.ReplicationStatus
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.adapters.AirbyteJsonRecordAdapter
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageTracker
import io.airbyte.container.orchestrator.bookkeeping.SyncStatsTracker
import io.airbyte.container.orchestrator.bookkeeping.events.ReplicationAirbyteMessageEventPublishingHelper
import io.airbyte.container.orchestrator.bookkeeping.streamstatus.StreamStatusTracker
import io.airbyte.container.orchestrator.tracker.AnalyticsMessageTracker
import io.airbyte.container.orchestrator.tracker.StreamStatusCompletionTracker
import io.airbyte.container.orchestrator.tracker.ThreadedTimeTracker
import io.airbyte.container.orchestrator.worker.filter.FieldSelector
import io.airbyte.mappers.application.RecordMapper
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteAnalyticsTraceMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.LOG
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.RECORD
import io.airbyte.protocol.models.v0.AirbyteMessage.Type.TRACE
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage.Type.ANALYTICS
import io.airbyte.workers.internal.AirbyteMapper
import io.mockk.MockKAnnotations
import io.mockk.Runs
import io.mockk.every
import io.mockk.impl.annotations.InjectMockKs
import io.mockk.impl.annotations.MockK
import io.mockk.impl.annotations.SpyK
import io.mockk.junit5.MockKExtension
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class ReplicationWorkerHelperTest {
  @MockK(relaxed = true)
  lateinit var fieldSelector: FieldSelector

  @MockK(relaxed = true)
  lateinit var mapper: AirbyteMapper

  @MockK(relaxed = true)
  lateinit var messageTracker: AirbyteMessageTracker

  @MockK(relaxed = true)
  lateinit var eventPublisher: ReplicationAirbyteMessageEventPublishingHelper

  @MockK(relaxed = true)
  lateinit var timeTracker: ThreadedTimeTracker

  @MockK(relaxed = true)
  lateinit var analyticsTracker: AnalyticsMessageTracker

  @MockK(relaxed = true)
  lateinit var streamStatusCompletionTracker: StreamStatusCompletionTracker

  @MockK(relaxed = true)
  lateinit var streamStatusTracker: StreamStatusTracker

  @MockK(relaxed = true)
  lateinit var recordMapper: RecordMapper

  @MockK(relaxed = true)
  lateinit var replicationWorkerState: ReplicationWorkerState

  @MockK(relaxed = true)
  lateinit var context: ReplicationContextProvider.Context

  @MockK(relaxed = true)
  lateinit var destinationCatalogGenerator: DestinationCatalogGenerator

  @MockK(relaxed = true)
  lateinit var metricClient: MetricClient

  @MockK(relaxed = true)
  lateinit var syncStatsTracker: SyncStatsTracker

  @InjectMockKs
  @SpyK
  lateinit var helper: ReplicationWorkerHelper

  @BeforeEach
  fun setUp() {
    MockKAnnotations.init(this)

    every { context.replicationContext } returns mockk(relaxed = true)
    every { context.supportRefreshes } returns false
    val configuredCatalog = mockk<ConfiguredAirbyteCatalog>(relaxed = true)
    every { context.configuredCatalog } returns configuredCatalog
    every { destinationCatalogGenerator.generateDestinationCatalog(configuredCatalog) } answers {
      DestinationCatalogGenerator.CatalogGenerationResult(configuredCatalog, emptyMap())
    }

    every { syncStatsTracker.getTotalBytesEmitted() } returns 0L
    every { syncStatsTracker.getTotalRecordsEmitted() } returns 0L
  }

  @Test
  fun `getReplicationOutput returns correct stats for completed sync`() {
    every { replicationWorkerState.cancelled } returns false
    every { replicationWorkerState.hasFailed } returns false

    every { syncStatsTracker.getTotalBytesCommitted() } returns 50L
    every { syncStatsTracker.getTotalRecordsCommitted() } returns 5L
    every { syncStatsTracker.getUnreliableStateTimingMetrics() } returns false

    val output = helper.getReplicationOutput()

    assertEquals(50L, output.replicationAttemptSummary.bytesSynced)
    assertEquals(5L, output.replicationAttemptSummary.recordsSynced)
    assertEquals(ReplicationStatus.COMPLETED, output.replicationAttemptSummary.status)
  }

  @Test
  fun `analytics messages are tracked and flushed`() {
    every { replicationWorkerState.cancelled } returns false
    every { replicationWorkerState.hasFailed } returns false
    every { mapper.mapMessage(any()) } answers { firstArg<AirbyteMessage>() }
    every { mapper.revertMap(any()) } answers { firstArg<AirbyteMessage>() }

    val sourceMsg =
      AirbyteMessage()
        .withType(TRACE)
        .withTrace(
          AirbyteTraceMessage()
            .withType(ANALYTICS)
            .withAnalytics(AirbyteAnalyticsTraceMessage().withType("from").withValue("src")),
        )
    val logMsg = AirbyteMessage().withType(LOG)
    val destMsg =
      AirbyteMessage()
        .withType(TRACE)
        .withTrace(
          AirbyteTraceMessage()
            .withType(ANALYTICS)
            .withAnalytics(AirbyteAnalyticsTraceMessage().withType("from").withValue("dest")),
        )

    helper.processMessageFromSource(sourceMsg)
    helper.processMessageFromSource(logMsg)
    helper.processMessageFromDestination(destMsg)
    helper.endOfReplication()

    verify(exactly = 1) { analyticsTracker.addMessage(sourceMsg, AirbyteMessageOrigin.SOURCE) }
    verify(exactly = 1) { analyticsTracker.addMessage(destMsg, AirbyteMessageOrigin.DESTINATION) }
    verify(exactly = 1) { analyticsTracker.flush() }
  }

  @Test
  fun `processMessageFromSource maps after internal processing`() {
    val raw = mockk<AirbyteMessage>(relaxed = true)
    val mapped = mockk<AirbyteMessage>(relaxed = true)
    every { helper.internalProcessMessageFromSource(raw) } returns raw
    every { mapper.mapMessage(raw) } returns mapped

    val result = helper.processMessageFromSource(raw)
    assertTrue(result.isPresent)
    assertEquals(mapped, result.get())
  }

  @Test
  fun `processMessageFromDestination reverts map before processing`() {
    val raw = mockk<AirbyteMessage>(relaxed = true)
    val reverted = mockk<AirbyteMessage>(relaxed = true)
    every { mapper.revertMap(raw) } returns reverted
    every { helper.internalProcessMessageFromDestination(reverted) } just Runs

    helper.processMessageFromDestination(raw)
    verify(exactly = 1) { helper.internalProcessMessageFromDestination(reverted) }
  }

  @Test
  fun `stream status tracker is called on source messages`() {
    val msg = mockk<AirbyteMessage>(relaxed = true)
    helper.internalProcessMessageFromSource(msg)
    verify { streamStatusTracker.track(msg) }
  }

  @Test
  fun `stream status tracker is called on destination messages`() {
    val msg = mockk<AirbyteMessage>(relaxed = true)
    every { mapper.revertMap(msg) } returns msg
    helper.internalProcessMessageFromDestination(msg)
    verify { streamStatusTracker.track(msg) }
  }

  @Test
  fun `applyTransformationMappers with no mappers does nothing`() {
    every { context.configuredCatalog.streams } returns emptyList()
    val adapter =
      AirbyteJsonRecordAdapter(
        AirbyteMessage()
          .withType(RECORD)
          .withRecord(AirbyteRecordMessage().withStream("s").withData(ObjectMapper().createObjectNode())),
      )
    helper.applyTransformationMappers(adapter)
    verify(exactly = 0) { recordMapper.applyMappers(any(), any<List<MapperConfig>>()) }
  }

  @Test
  fun `applyTransformationMappers with configured mappers applies them`() {
    val streamDescriptor =
      StreamDescriptor()
        .withName("s")
    val mapperConfig =
      object : MapperConfig {
        override fun name() = "test"

        override fun documentationUrl(): String? = null

        override fun id(): UUID? = null

        override fun config(): Any = emptyMap<String, String>()
      }
    val configuredStream = mockk<ConfiguredAirbyteStream>(relaxed = true)
    every { configuredStream.streamDescriptor } returns streamDescriptor
    every { configuredStream.mappers } returns listOf(mapperConfig)
    every { context.configuredCatalog.streams } returns listOf(configuredStream)
    every { destinationCatalogGenerator.generateDestinationCatalog(any()) } answers {
      DestinationCatalogGenerator.CatalogGenerationResult(context.configuredCatalog, emptyMap())
    }
    helper =
      spyk(
        ReplicationWorkerHelper(
          fieldSelector,
          mapper,
          messageTracker,
          eventPublisher,
          timeTracker,
          analyticsTracker,
          streamStatusCompletionTracker,
          syncStatsTracker,
          streamStatusTracker,
          recordMapper,
          replicationWorkerState,
          context,
          destinationCatalogGenerator,
          metricClient,
        ),
        recordPrivateCalls = true,
      )

    val adapter =
      AirbyteJsonRecordAdapter(
        AirbyteMessage()
          .withType(RECORD)
          .withRecord(AirbyteRecordMessage().withStream("s").withData(ObjectMapper().createObjectNode())),
      )
    helper.applyTransformationMappers(adapter)

    verify(exactly = 1) { recordMapper.applyMappers(adapter, listOf(mapperConfig)) }
  }
}
