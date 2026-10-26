/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SourceApi
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.models.SidecarInput
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import org.openapitools.client.infrastructure.ClientException
import java.io.InputStream
import java.util.UUID

/**
 * When the platform API refuses the discovered catalog, the failure message must say so.
 * Before, a 413 surfaced only as "Unexpected error performing DISCOVER", with the cause buried in the stack trace.
 */
@ExtendWith(MockKExtension::class)
class ConnectorMessageProcessorApiErrorTest {
  @MockK
  private lateinit var airbyteApiClient: AirbyteApiClient

  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  @MockK
  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater

  @MockK
  private lateinit var sourceApi: SourceApi

  private lateinit var connectorMessageProcessor: ConnectorMessageProcessor

  @BeforeEach
  fun init() {
    every { airbyteApiClient.sourceApi } returns sourceApi
    every { streamFactory.create(any(), any()) } returns
      listOf(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.CATALOG)
          .withCatalog(AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("name")))),
      ).stream()
    connectorMessageProcessor =
      ConnectorMessageProcessor(
        connectorConfigUpdater,
        airbyteApiClient,
        CatalogClientConverters(FieldGenerator()),
        mockk(relaxed = true),
        mockk(relaxed = true),
      )
  }

  @Test
  fun `a 413 from the catalog write names the status and says the result is too large`() {
    val rejected = ClientException(message = "Client error : 413 Request Entity Too Large", statusCode = 413)
    every { sourceApi.writeDiscoverCatalogResult(any()) } throws rejected

    val failure = assertThrows<WorkerException> { runDiscover() }

    assertEquals(
      "The platform API rejected the DISCOVER result with HTTP 413: Client error : 413 Request Entity Too Large. " +
        "The result is larger than the platform API accepts.",
      failure.message,
    )
    assertSame(rejected, failure.cause)
  }

  @Test
  fun `another client error from the catalog write names the status without the size hint`() {
    every { sourceApi.writeDiscoverCatalogResult(any()) } throws
      ClientException(message = "Client error : 409 Conflict", statusCode = 409)

    val failure = assertThrows<WorkerException> { runDiscover() }

    assertEquals("The platform API rejected the DISCOVER result with HTTP 409: Client error : 409 Conflict.", failure.message)
  }

  @Test
  fun `any other failure keeps the exit code and adds the cause`() {
    every { sourceApi.writeDiscoverCatalogResult(any()) } throws IllegalStateException("catalog store unavailable")

    val failure = assertThrows<WorkerException> { runDiscover() }

    assertEquals(
      "Unexpected error performing DISCOVER. The exit of the connector was: 0. Cause: catalog store unavailable",
      failure.message,
    )
  }

  private fun runDiscover() =
    connectorMessageProcessor.run(
      InputStream.nullInputStream(),
      streamFactory,
      ConnectorMessageProcessor.OperationInput(
        discoveryInput =
          StandardDiscoverCatalogInput()
            .withConnectionConfiguration(Jsons.emptyObject())
            .withSourceId(UUID.randomUUID().toString()),
      ),
      0,
      SidecarInput.OperationType.DISCOVER,
    )
}
