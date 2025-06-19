/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.events

import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.container.orchestrator.bookkeeping.AirbyteMessageOrigin
import io.airbyte.container.orchestrator.worker.context.ReplicationContext
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
import java.io.IOException
import java.util.UUID

internal class AirbyteControlMessageEventListenerTest {
  private lateinit var messageEventListener: AirbyteControlMessageEventListener
  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater

  @BeforeEach
  fun setup() {
    connectorConfigUpdater =
      mockk {
        every { updateDestination(any(), any()) } returns Unit
        every { updateSource(any(), any()) } returns Unit
      }
    messageEventListener = AirbyteControlMessageEventListener(connectorConfigUpdater)
  }

  @Test
  @Throws(IOException::class)
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
  @Throws(IOException::class)
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
  @Throws(IOException::class)
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
}
