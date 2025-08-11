/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import io.airbyte.commons.server.handlers.AssistProxyHandler
import io.airbyte.commons.server.handlers.ConnectorContributionHandler
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifestRequestBody
import io.airbyte.connectorbuilder.api.model.generated.StreamRead
import io.airbyte.connectorbuilder.api.model.generated.StreamReadRequestBody
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException
import io.airbyte.connectorbuilder.handlers.FullResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.airbyte.connectorbuilder.handlers.ResolveManifestHandler
import io.airbyte.connectorbuilder.handlers.StreamHandler
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class ConnectorBuilderControllerTest {
  private lateinit var controller: ConnectorBuilderController
  private var healthHandler: HealthHandler = mockk()
  private var streamHandler: StreamHandler = mockk()
  private var resolveManifestHandler: ResolveManifestHandler = mockk()
  private var fullResolveManifestHandler: FullResolveManifestHandler = mockk()
  private var streamReadRequestBody: StreamReadRequestBody = mockk()
  private var streamReadResponse: StreamRead = mockk()
  private var resolveManifestRequestBody: ResolveManifestRequestBody = mockk()
  private var resolveManifest: ResolveManifest = mockk()
  private var connectorContributionHandler: ConnectorContributionHandler = mockk()
  private var assistProxyHandler: AssistProxyHandler = mockk()

  @BeforeEach
  fun setup() {
    clearAllMocks()

    controller =
      ConnectorBuilderController(
        healthHandler = healthHandler,
        resolveManifestHandler = resolveManifestHandler,
        fullResolveManifestHandler = fullResolveManifestHandler,
        streamHandler = streamHandler,
        connectorContributionHandler = connectorContributionHandler,
        assistProxyHandler = assistProxyHandler,
      )
  }

  @Test
  fun whenReadStreamThenReturnHandlerResponse() {
    every { streamHandler.readStream(streamReadRequestBody) } answers { streamReadResponse }
    val response = controller.readStream(streamReadRequestBody)
    assertEquals(streamReadResponse, response)
  }

  @Test
  fun givenExceptionWhenReadStreamThenThrowSameException() {
    every { streamHandler.readStream(any()) } throws AirbyteCdkInvalidInputException("message")
    assertThrows<AirbyteCdkInvalidInputException> { controller.readStream(streamReadRequestBody) }
  }

  @Test
  fun whenResolveManifestThenReturnHandlerResponse() {
    every { resolveManifestHandler.resolveManifest(resolveManifestRequestBody) } answers { resolveManifest }
    val response = controller.resolveManifest(resolveManifestRequestBody)
    assertEquals(resolveManifest, response)
  }

  @Test
  fun givenExceptionWhenResolveManifestThenThrowSameException() {
    every { resolveManifestHandler.resolveManifest(any()) } throws AirbyteCdkInvalidInputException("message")
    assertThrows<AirbyteCdkInvalidInputException> {
      controller.resolveManifest(resolveManifestRequestBody)
    }
  }
}
