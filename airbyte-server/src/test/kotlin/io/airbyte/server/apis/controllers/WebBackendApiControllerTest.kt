/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.api.model.generated.WebBackendConnectionCreate
import io.airbyte.api.model.generated.WebBackendConnectionListRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionReadList
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendConnectionUpdate
import io.airbyte.api.model.generated.WebBackendWorkspaceState
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

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
        webappConfig = WebappConfig(version = "1.1.1", edition = "community", webApp = emptyMap()),
      )
  }

  @Test
  fun testGetStateType() {
    every { webBackendConnectionsHandler.getStateType(any()) } returns ConnectionStateType.STREAM

    val sourceIdRequestBody = ConnectionIdRequestBody()
    val result = controller.getStateType(sourceIdRequestBody)

    assert(result == ConnectionStateType.STREAM)
  }

  @Test
  fun testWebBackendCheckUpdates() {
    every { webBackendCheckUpdatesHandler.checkUpdates() } returns WebBackendCheckUpdatesRead()

    val result = controller.webBackendCheckUpdates()

    assert(result != null)
  }

  @Test
  fun testWebBackendCreateConnection() {
    every { webBackendConnectionsHandler.webBackendCreateConnection(any()) } returns WebBackendConnectionRead()

    val sourceIdRequestBody =
      io.airbyte.api.model.generated
        .WebBackendConnectionCreate()
    val result = controller.webBackendCreateConnection(sourceIdRequestBody)

    assert(result != null)
  }

  @Test
  @Disabled("Complex test with multiple mock calls - simplified for direct controller testing")
  fun testWebBackendGetConnection() {
    every { webBackendConnectionsHandler.webBackendGetConnection(any()) } returns WebBackendConnectionRead()

    val requestBody = WebBackendConnectionRequestBody()
    val result = controller.webBackendGetConnection(requestBody)

    assert(result != null)
  }

  @Test
  fun testWebBackendGetWorkspaceState() {
    every { webBackendConnectionsHandler.getWorkspaceState(any()) } returns WebBackendWorkspaceStateResult()

    val sourceIdRequestBody = WebBackendWorkspaceState()
    val result = controller.webBackendGetWorkspaceState(sourceIdRequestBody)

    assert(result != null)
  }

  @Test
  fun testWebBackendListConnectionsForWorkspace() {
    every { webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(any()) } returns WebBackendConnectionReadList()

    val sourceIdRequestBody = WebBackendConnectionListRequestBody()
    val result = controller.webBackendListConnectionsForWorkspace(sourceIdRequestBody)

    assert(result != null)
  }

  @Test
  fun testWebBackendUpdateConnection() {
    every { webBackendConnectionsHandler.webBackendUpdateConnection(any()) } returns WebBackendConnectionRead()

    val sourceIdRequestBody = WebBackendConnectionUpdate()
    val result = controller.webBackendUpdateConnection(sourceIdRequestBody)

    assert(result != null)
  }

  @Test
  fun `test config`() {
    val result = controller.getWebappConfig()

    assert(result != null)
  }
}
