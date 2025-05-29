/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import io.airbyte.config.FailureReason
import io.airbyte.container.orchestrator.persistence.SyncPersistence
import io.airbyte.container.orchestrator.worker.context.ReplicationInputFeatureFlagReader
import io.airbyte.featureflag.LogConnectorMessages
import io.airbyte.featureflag.LogStateMsgs
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.Config
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.workers.helper.FailureHelper.destinationFailure
import io.airbyte.workers.helper.FailureHelper.sourceFailure
import io.airbyte.workers.models.ArchitectureConstants
import io.airbyte.workers.testutils.AirbyteMessageUtils
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class AirbyteMessageTrackerTest {
  private lateinit var messageTracker: AirbyteMessageTracker
  private lateinit var syncPersistence: SyncPersistence
  private lateinit var replicationInputFeatureFlagReader: ReplicationInputFeatureFlagReader
  private lateinit var replicationInput: ReplicationInput

  @BeforeEach
  fun setup() {
    replicationInput =
      ReplicationInput()
        .withConnectionId(UUID.randomUUID())
        .withDestinationLauncherConfig(IntegrationLauncherConfig().withDockerImage("airbyte/destination-image"))
        .withSourceLauncherConfig(IntegrationLauncherConfig().withDockerImage("airbyte/source-image"))
    replicationInputFeatureFlagReader =
      mockk {
        every { read(LogConnectorMessages) } returns false
        every { read(LogStateMsgs) } returns false
      }
    syncPersistence = mockk(relaxed = true)

    this.messageTracker =
      AirbyteMessageTracker(
        replicationInputFeatureFlagReader = replicationInputFeatureFlagReader,
        replicationInput = replicationInput,
        syncPersistence = syncPersistence,
        platformMode = ArchitectureConstants.ORCHESTRATOR,
      )
  }

  @Test
  fun testAcceptFromSourceTraceEstimate() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.ESTIMATE)

    messageTracker.acceptFromSource(trace)

    verify(exactly = 1) { syncPersistence.updateEstimates((trace.trace.estimate)) }
  }

  @Test
  fun testAcceptFromSourceTraceError() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.ERROR)

    messageTracker.acceptFromSource(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromSourceTraceStreamStatus() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.STREAM_STATUS)

    messageTracker.acceptFromSource(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromSourceTraceEstimateOther() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, null as AirbyteTraceMessage.Type?)

    messageTracker.acceptFromSource(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromSourceRecord() {
    val record = AirbyteMessageUtils.createRecordMessage("stream 1", 123)

    messageTracker.acceptFromSource(record)

    verify(exactly = 1) { syncPersistence.updateStats(record.record) }
  }

  @Test
  fun testAcceptFromSourceState() {
    val state = AirbyteMessageUtils.createStateMessage(2)

    messageTracker.acceptFromSource(state)

    verify(exactly = 0) { syncPersistence.accept(replicationInput.connectionId, state.state) }
    verify(exactly = 1) { syncPersistence.updateSourceStatesStats(state.state) }
  }

  @Test
  fun testAcceptFromSourceControl() {
    val config = mockk<Config>(relaxed = true)
    val control = AirbyteMessageUtils.createConfigControlMessage(config, 0.0)

    messageTracker.acceptFromSource(control!!)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationTraceEstimate() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace =
      AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.ESTIMATE)

    messageTracker.acceptFromDestination(trace)

    verify(exactly = 1) { syncPersistence.updateEstimates((trace.trace.estimate)) }
  }

  @Test
  fun testAcceptFromDestinationTraceAnalytics() {
    val trace = AirbyteMessageUtils.createAnalyticsTraceMessage("abc", "def")

    messageTracker.acceptFromDestination(trace!!)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromSourceTraceAnalytics() {
    val trace = AirbyteMessageUtils.createAnalyticsTraceMessage("abc", "def")

    messageTracker.acceptFromSource(trace!!)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationTraceError() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.ERROR)

    messageTracker.acceptFromDestination(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationTraceStreamStatus() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace = AirbyteMessageUtils.createStatusTraceMessage(stream, AirbyteTraceMessage.Type.STREAM_STATUS)

    messageTracker.acceptFromDestination(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationTraceEstimateOther() {
    val stream = mockk<StreamDescriptor>(relaxed = true)
    val trace =
      AirbyteMessageUtils.createStatusTraceMessage(stream, null as AirbyteTraceMessage.Type?)

    messageTracker.acceptFromDestination(trace)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationRecord() {
    val record = AirbyteMessageUtils.createRecordMessage("record 1", 123)

    messageTracker.acceptFromDestination(record)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testAcceptFromDestinationState() {
    val state = AirbyteMessageUtils.createStateMessage(2)

    messageTracker.acceptFromDestination(state)

    verify(exactly = 1) { syncPersistence.accept(replicationInput.connectionId, state.state) }
    verify(exactly = 1) { syncPersistence.updateDestinationStateStats(state.state) }
  }

  @Test
  fun testAcceptFromDestinationControl() {
    val config = mockk<Config>(relaxed = true)
    val control = AirbyteMessageUtils.createConfigControlMessage(config, 0.0)

    messageTracker.acceptFromDestination(control!!)

    verify { syncPersistence wasNot Called }
  }

  @Test
  fun testErrorTraceMessageFailureWithMultipleTraceErrors() {
    val srcMsg1 = AirbyteMessageUtils.createErrorMessage("source trace 1", 123.0)
    val srcMsg2 = AirbyteMessageUtils.createErrorMessage("source trace 2", 124.0)
    val dstMsg1 = AirbyteMessageUtils.createErrorMessage("dest trace 1", 125.0)
    val dstMsg2 = AirbyteMessageUtils.createErrorMessage("dest trace 2", 126.0)

    messageTracker.acceptFromSource(srcMsg1)
    messageTracker.acceptFromSource(srcMsg2)
    messageTracker.acceptFromDestination(dstMsg1)
    messageTracker.acceptFromDestination(dstMsg2)

    val failureReasons: MutableList<FailureReason> = mutableListOf()
    failureReasons.addAll(
      listOf(srcMsg1, srcMsg2).map { m: AirbyteMessage -> sourceFailure(m.trace, 123, 1) }.toList(),
    )
    failureReasons.addAll(
      listOf(dstMsg1, dstMsg2).map { m: AirbyteMessage -> destinationFailure(m.trace, 123, 1) }.toList(),
    )
    assertEquals(failureReasons, messageTracker.errorTraceMessageFailure(123L, 1))
  }

  @Test
  fun testErrorTraceMessageFailureWithOneTraceError() {
    val destMessage = AirbyteMessageUtils.createErrorMessage("dest trace 1", 125.0)
    messageTracker.acceptFromDestination(destMessage)

    val failureReason = destinationFailure(destMessage.trace, 123, 1)
    assertEquals(listOf(failureReason), messageTracker.errorTraceMessageFailure(123L, 1))
  }

  @Test
  fun testErrorTraceMessageFailureWithNoTraceErrors() {
    assertTrue(messageTracker.errorTraceMessageFailure(123L, 1).isEmpty())
  }
}
