/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteControlConnectorConfigMessage
import io.airbyte.protocol.models.v0.AirbyteControlMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.Config
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class AirbyteControlMessageEventListenerTest {
  private lateinit var messageEventListener: AirbyteControlMessageEventListener
  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater
  private lateinit var metricClient: MetricClient

  @BeforeEach
  fun setup() {
    connectorConfigUpdater =
      mockk {
        every { updateDestination(any(), any()) } returns Unit
        every { updateSource(any(), any()) } returns Unit
      }
    metricClient = mockk(relaxed = true)
    messageEventListener = AirbyteControlMessageEventListener(connectorConfigUpdater, metricClient)
  }

  @Test
  fun testDestinationControlMessage() {
    val destinationUUID = UUID.randomUUID()
    val airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION
    val configObj = mockk<Config>()
    val airbyteControlConnectorConfigMessage =
      mockk<AirbyteControlConnectorConfigMessage> {
        every { config } returns configObj
      }
    val airbyteControlMessage =
      mockk<AirbyteControlMessage> {
        every { connectorConfig } returns airbyteControlConnectorConfigMessage
        every { type } returns AirbyteControlMessage.Type.CONNECTOR_CONFIG
      }
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { control } returns airbyteControlMessage
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext =
      mockk<ReplicationContext> {
        every { destinationId } returns destinationUUID
      }

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent)

    verify(exactly = 1) { connectorConfigUpdater.updateDestination(destinationUUID, configObj) }
  }

  @Test
  fun testSourceControlMessage() {
    val sourceUUID = UUID.randomUUID()
    val airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE
    val configObj = mockk<Config>()
    val airbyteControlConnectorConfigMessage =
      mockk<AirbyteControlConnectorConfigMessage> {
        every { config } returns configObj
      }
    val airbyteControlMessage =
      mockk<AirbyteControlMessage> {
        every { connectorConfig } returns airbyteControlConnectorConfigMessage
        every { type } returns AirbyteControlMessage.Type.CONNECTOR_CONFIG
      }
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { control } returns airbyteControlMessage
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext =
      mockk<ReplicationContext> {
        every { sourceId } returns sourceUUID
      }

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent)

    verify(exactly = 1) { connectorConfigUpdater.updateSource(sourceUUID, configObj) }
  }

  @Test
  fun testInternalControlMessage() {
    val airbyteMessageOrigin = AirbyteMessageOrigin.INTERNAL
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext = mockk<ReplicationContext>()

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    messageEventListener.onApplicationEvent(replicationAirbyteMessageEvent)

    verify(exactly = 0) { connectorConfigUpdater.updateDestination(any(), any()) }
    verify(exactly = 0) { connectorConfigUpdater.updateSource(any(), any()) }
  }

  @Test
  fun testSupportsDestinationEvent() {
    val airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext = mockk<ReplicationContext>()

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent))
  }

  @Test
  fun testSupportsSourceEvent() {
    val airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext = mockk<ReplicationContext>()

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    assertTrue(messageEventListener.supports(replicationAirbyteMessageEvent))
  }

  @Test
  fun testDoesNotSupportNonControlEvent() {
    val airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { type } returns AirbyteMessage.Type.STATE
      }
    val replicationContext = mockk<ReplicationContext>()

    val replicationAirbyteMessageEvent =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = airbyteMessageOrigin,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    assertFalse(messageEventListener.supports(replicationAirbyteMessageEvent))
  }

  @Test
  fun testSourceConfigUpdateFailureDoesNotThrow() {
    val sourceUUID = UUID.randomUUID()
    val connectionUUID = UUID.randomUUID()
    val testJobId = 42L
    val configObj = mockk<Config>()
    val airbyteControlConnectorConfigMessage =
      mockk<AirbyteControlConnectorConfigMessage> {
        every { config } returns configObj
      }
    val airbyteControlMessage =
      mockk<AirbyteControlMessage> {
        every { connectorConfig } returns airbyteControlConnectorConfigMessage
        every { type } returns AirbyteControlMessage.Type.CONNECTOR_CONFIG
      }
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { control } returns airbyteControlMessage
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext =
      mockk<ReplicationContext> {
        every { sourceId } returns sourceUUID
        every { connectionId } returns connectionUUID
        every { jobId } returns testJobId
      }

    every { connectorConfigUpdater.updateSource(sourceUUID, configObj) } throws RuntimeException("API call failed")

    val event =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = AirbyteMessageOrigin.SOURCE,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    // Should not throw — failure is caught and logged
    messageEventListener.onApplicationEvent(event)

    verify(exactly = 1) { connectorConfigUpdater.updateSource(sourceUUID, configObj) }
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.CONNECTOR_CONFIG_PERSISTENCE_FAILURE,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.CONNECTION_ID, connectionUUID.toString()),
            MetricAttribute(MetricTags.SOURCE_ID, sourceUUID.toString()),
            MetricAttribute(MetricTags.JOB_ID, testJobId.toString()),
          ),
      )
    }
  }

  @Test
  fun testDestinationConfigUpdateFailureDoesNotThrow() {
    val destinationUUID = UUID.randomUUID()
    val connectionUUID = UUID.randomUUID()
    val testJobId = 99L
    val configObj = mockk<Config>()
    val airbyteControlConnectorConfigMessage =
      mockk<AirbyteControlConnectorConfigMessage> {
        every { config } returns configObj
      }
    val airbyteControlMessage =
      mockk<AirbyteControlMessage> {
        every { connectorConfig } returns airbyteControlConnectorConfigMessage
        every { type } returns AirbyteControlMessage.Type.CONNECTOR_CONFIG
      }
    val airbyteMessage =
      mockk<AirbyteMessage> {
        every { control } returns airbyteControlMessage
        every { type } returns AirbyteMessage.Type.CONTROL
      }
    val replicationContext =
      mockk<ReplicationContext> {
        every { destinationId } returns destinationUUID
        every { connectionId } returns connectionUUID
        every { jobId } returns testJobId
      }

    every { connectorConfigUpdater.updateDestination(destinationUUID, configObj) } throws RuntimeException("API call failed")

    val event =
      ReplicationAirbyteMessageEvent(
        airbyteMessageOrigin = AirbyteMessageOrigin.DESTINATION,
        airbyteMessage = airbyteMessage,
        replicationContext = replicationContext,
      )

    // Should not throw — failure is caught and logged
    messageEventListener.onApplicationEvent(event)

    verify(exactly = 1) { connectorConfigUpdater.updateDestination(destinationUUID, configObj) }
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.CONNECTOR_CONFIG_PERSISTENCE_FAILURE,
        attributes =
          arrayOf(
            MetricAttribute(MetricTags.CONNECTION_ID, connectionUUID.toString()),
            MetricAttribute(MetricTags.DESTINATION_ID, destinationUUID.toString()),
            MetricAttribute(MetricTags.JOB_ID, testJobId.toString()),
          ),
      )
    }
  }
}
