/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.converters

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.DestinationApi
import io.airbyte.api.client.generated.SourceApi
import io.airbyte.api.client.model.generated.DestinationIdRequestBody
import io.airbyte.api.client.model.generated.DestinationRead
import io.airbyte.api.client.model.generated.DestinationUpdate
import io.airbyte.api.client.model.generated.SourceIdRequestBody
import io.airbyte.api.client.model.generated.SourceRead
import io.airbyte.api.client.model.generated.SourceUpdate
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.Config
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.UUID

private val SOURCE_ID: UUID = UUID.randomUUID()
private const val SOURCE_NAME = "source-stripe"
private val DESTINATION_ID: UUID = UUID.randomUUID()
private const val DESTINATION_NAME = "destination-google-sheets"

internal class ConnectorConfigUpdaterTest {
  private val mAirbyteApiClient: AirbyteApiClient = mockk()
  private val mSourceApi: SourceApi = mockk()
  private val mDestinationApi: DestinationApi = mockk()

  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater

  @BeforeEach
  @Throws(IOException::class)
  fun setUp() {
    every { mSourceApi.getSource(SourceIdRequestBody(SOURCE_ID)) } returns
      SourceRead(
        sourceDefinitionId = UUID.randomUUID(),
        sourceId = SOURCE_ID,
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.jsonNode(mapOf<Any, Any>()),
        name = SOURCE_NAME,
        sourceName = SOURCE_NAME,
        createdAt = 1L,
      )

    every { mDestinationApi.getDestination(DestinationIdRequestBody(DESTINATION_ID)) } returns
      DestinationRead(
        destinationDefinitionId = UUID.randomUUID(),
        destinationId = DESTINATION_ID,
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.jsonNode(mapOf<Any, Any>()),
        name = DESTINATION_NAME,
        destinationName = DESTINATION_NAME,
        createdAt = 1L,
      )

    every { mAirbyteApiClient.destinationApi } returns mDestinationApi
    every { mAirbyteApiClient.sourceApi } returns mSourceApi

    connectorConfigUpdater = ConnectorConfigUpdater(mAirbyteApiClient)
  }

  @Test
  @Throws(IOException::class)
  fun testPersistSourceConfig() {
    val newConfiguration = Config().withAdditionalProperty("key", "new_value")
    val configJson = Jsons.jsonNode(newConfiguration.additionalProperties)

    val expectedSourceUpdate = SourceUpdate(SOURCE_ID, configJson, SOURCE_NAME, null, null)

    every { mSourceApi.updateSource(any()) } returns
      SourceRead(
        sourceDefinitionId = UUID.randomUUID(),
        sourceId = SOURCE_ID,
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = configJson,
        name = SOURCE_NAME,
        sourceName = SOURCE_NAME,
        createdAt = 1L,
      )

    connectorConfigUpdater.updateSource(SOURCE_ID, newConfiguration)
    verify { mSourceApi.updateSource(expectedSourceUpdate) }
  }

  @Test
  @Throws(IOException::class)
  fun testPersistDestinationConfig() {
    val newConfiguration = Config().withAdditionalProperty("key", "new_value")
    val configJson = Jsons.jsonNode(newConfiguration.additionalProperties)

    val expectedDestinationUpdate = DestinationUpdate(DESTINATION_ID, configJson, DESTINATION_NAME, null)
    val destinationRead =
      DestinationRead(
        destinationDefinitionId = UUID.randomUUID(),
        destinationId = DESTINATION_ID,
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = configJson,
        name = DESTINATION_NAME,
        destinationName = DESTINATION_NAME,
        createdAt = 1L,
      )

    every { mDestinationApi.getDestination(DestinationIdRequestBody(DESTINATION_ID)) } returns destinationRead
    every { mDestinationApi.updateDestination(expectedDestinationUpdate) } returns destinationRead

    connectorConfigUpdater.updateDestination(DESTINATION_ID, newConfiguration)
    verify { mDestinationApi.updateDestination(expectedDestinationUpdate) }
  }
}
