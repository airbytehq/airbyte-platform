/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.ConnectionStateType
import io.airbyte.api.server.generated.models.ConnectionStatus
import io.airbyte.api.server.generated.models.WebBackendCheckUpdatesRead
import io.airbyte.api.server.generated.models.WebBackendConnectionListRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionRead
import io.airbyte.api.server.generated.models.WebBackendConnectionReadList
import io.airbyte.api.server.generated.models.WebBackendConnectionRequestBody
import io.airbyte.api.server.generated.models.WebBackendConnectionUpdate
import io.airbyte.api.server.generated.models.WebBackendWorkspaceState
import io.airbyte.api.server.generated.models.WebBackendWorkspaceStateResult
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.config.Configs
import io.airbyte.micronaut.runtime.AirbyteConfig
import io.airbyte.micronaut.runtime.AirbyteWebappConfig
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

internal class WebBackendApiControllerTest {
  private lateinit var controller: WebBackendApiController
  private val roleResolver: RoleResolver = mockk()
  private val webBackendConnectionsHandler: WebBackendConnectionsHandler = mockk()
  private val webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler = mockk()
  private val webBackendCronExpressionHandler: WebBackendCronExpressionHandler = mockk()

  @BeforeEach
  fun setUp() {
    controller =
      WebBackendApiController(
        webBackendConnectionsHandler = webBackendConnectionsHandler,
        webBackendCheckUpdatesHandler = webBackendCheckUpdatesHandler,
        webBackendCronExpressionHandler = webBackendCronExpressionHandler,
        roleResolver = roleResolver,
        webBackendMappersHandler = mockk(),
        airbyteConfig = AirbyteConfig(version = "1.1.1", edition = Configs.AirbyteEdition.COMMUNITY),
        airbyteWebappConfig = AirbyteWebappConfig(),
      )
  }

  @Test
  fun testGetStateType() {
    every { webBackendConnectionsHandler.getStateType(any()) } returns ConnectionStateType.STREAM

    val sourceIdRequestBody = ConnectionIdRequestBody(connectionId = UUID.randomUUID())
    val result = controller.getStateType(sourceIdRequestBody)

    assert(result == ConnectionStateType.STREAM)
  }

  @Test
  fun testWebBackendCheckUpdates() {
    val expected = WebBackendCheckUpdatesRead(destinationDefinitions = 0, sourceDefinitions = 0)
    every { webBackendCheckUpdatesHandler.checkUpdates() } returns expected

    val result = controller.webBackendCheckUpdates()

    assert(result == expected)
  }

  @Test
  fun testWebBackendCreateConnection() {
    val expected = mockk<WebBackendConnectionRead>()
    every { webBackendConnectionsHandler.webBackendCreateConnection(any()) } returns expected

    val sourceIdRequestBody =
      io.airbyte.api.server.generated.models.WebBackendConnectionCreate(
        sourceId = UUID.randomUUID(),
        destinationId = UUID.randomUUID(),
        status = ConnectionStatus.ACTIVE,
      )
    val result = controller.webBackendCreateConnection(sourceIdRequestBody)

    assert(result == expected)
  }

  @Test
  @Disabled("Complex test with multiple mock calls - simplified for direct controller testing")
  fun testWebBackendGetConnection() {
    val expected = mockk<WebBackendConnectionRead>()
    every { webBackendConnectionsHandler.webBackendGetConnection(any()) } returns expected

    val requestBody = WebBackendConnectionRequestBody(connectionId = UUID.randomUUID())
    val result = controller.webBackendGetConnection(requestBody)

    assert(result == expected)
  }

  @Test
  fun testWebBackendGetWorkspaceState() {
    val expected = WebBackendWorkspaceStateResult(hasConnections = false, hasSources = false, hasDestinations = false)
    every { webBackendConnectionsHandler.getWorkspaceState(any()) } returns expected

    val sourceIdRequestBody = WebBackendWorkspaceState(workspaceId = UUID.randomUUID())
    val result = controller.webBackendGetWorkspaceState(sourceIdRequestBody)

    assert(result == expected)
  }

  @Test
  fun testWebBackendListConnectionsForWorkspace() {
    val expected = WebBackendConnectionReadList(connections = emptyList())
    every { webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(any()) } returns expected

    val sourceIdRequestBody = WebBackendConnectionListRequestBody(workspaceId = UUID.randomUUID())
    val result = controller.webBackendListConnectionsForWorkspace(sourceIdRequestBody)

    assert(result == expected)
  }

  @Test
  fun testWebBackendUpdateConnection() {
    val expected = mockk<WebBackendConnectionRead>()
    every { webBackendConnectionsHandler.webBackendUpdateConnection(any()) } returns expected

    val sourceIdRequestBody = WebBackendConnectionUpdate(connectionId = UUID.randomUUID())
    val result = controller.webBackendUpdateConnection(sourceIdRequestBody)

    assert(result == expected)
  }

  @Test
  fun `test config`() {
    controller.getWebappConfig()
  }
}
