/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.WebBackendCheckUpdatesRead
import io.airbyte.api.model.generated.WebBackendConnectionRead
import io.airbyte.api.model.generated.WebBackendConnectionReadList
import io.airbyte.api.model.generated.WebBackendConnectionRequestBody
import io.airbyte.api.model.generated.WebBackendGeographiesListResult
import io.airbyte.api.model.generated.WebBackendWorkspaceStateResult
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.handlers.WebBackendCheckUpdatesHandler
import io.airbyte.commons.server.handlers.WebBackendConnectionsHandler
import io.airbyte.commons.server.handlers.WebBackendGeographiesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.handlers.WebBackendCronExpressionHandler
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(rebuildContext = true)
internal class WebBackendApiControllerTest {
  @Inject
  lateinit var apiAuthorizationHelper: ApiAuthorizationHelper

  @Inject
  lateinit var webBackendConnectionsHandler: WebBackendConnectionsHandler

  @Inject
  lateinit var webBackendCheckUpdatesHandler: WebBackendCheckUpdatesHandler

  @Inject
  lateinit var webBackendGeographiesHandler: WebBackendGeographiesHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(ApiAuthorizationHelper::class)
  fun apiAuthorizationHelper(): ApiAuthorizationHelper = mockk(relaxed = true)

  @MockBean(WebBackendConnectionsHandler::class)
  fun webBackendConnectionsHandler(): WebBackendConnectionsHandler = mockk()

  @MockBean(WebBackendCheckUpdatesHandler::class)
  fun webBackendCheckUpdatesHandler(): WebBackendCheckUpdatesHandler = mockk()

  @MockBean(WebBackendGeographiesHandler::class)
  fun webBackendGeographiesHandler(): WebBackendGeographiesHandler = mockk()

  @MockBean(WebBackendCronExpressionHandler::class)
  fun webBackendCronExpressionHandler(): WebBackendCronExpressionHandler = mockk()

  @MockBean(CurrentUserService::class)
  fun currentUserService(): CurrentUserService = mockk(relaxed = true) {}

  @Test
  fun testGetStateType() {
    every { webBackendConnectionsHandler.getStateType(any()) } returns ConnectionStateType.STREAM

    val path = "/api/v1/web_backend/state/get_type"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testWebBackendCheckUpdates() {
    every { webBackendCheckUpdatesHandler.checkUpdates() } returns WebBackendCheckUpdatesRead()
    val path = "/api/v1/web_backend/check_updates"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testWebBackendCreateConnection() {
    every { webBackendConnectionsHandler.webBackendCreateConnection(any()) } returns WebBackendConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/web_backend/connections/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  @Disabled("fails for unknown reasons")
  fun testWebBackendGetConnection() {
    // first two calls succeed, third call will fail
    every {
      webBackendConnectionsHandler.webBackendGetConnection(any())
    } returns WebBackendConnectionRead() andThen WebBackendConnectionRead() andThenThrows ConfigNotFoundException("", "")

    // This only impacts calls where withRefreshCatalog(true) is present
    // first two calls succeed, third call will fail
    every {
      apiAuthorizationHelper.checkWorkspacesPermissions(any(), any(), any(), any())
    } returns Unit andThen Unit andThenThrows ForbiddenProblem()

    val path = "/api/v1/web_backend/connections/get"

    // first call doesn't activate checkWorkspacePermissions because withRefreshedCatalog is false
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WebBackendConnectionRequestBody())))

//    // second call activates checkWorkspacePermissions because withRefreshedCatalog is true, and passes
//    // the check
    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST(path, WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true))),
    )
//
//    // third call activates checkWorkspacePermissions because withRefreshedCatalog is true, passes it,
//    // but then fails on the 404
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(HttpRequest.POST(path, WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true))),
    )
//
//    // fourth call activates checkWorkspacePermissions because withRefreshedCatalog is true, but fails
//    // the check, so 403s
    assertStatus(
      HttpStatus.FORBIDDEN,
      client.statusException(HttpRequest.POST(path, WebBackendConnectionRequestBody().connectionId(UUID.randomUUID()).withRefreshedCatalog(true))),
    )
  }

  @Test
  fun testWebBackendGetWorkspaceState() {
    every { webBackendConnectionsHandler.getWorkspaceState(any()) } returns WebBackendWorkspaceStateResult()

    val path = "/api/v1/web_backend/workspace/state"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testWebBackendListConnectionsForWorkspace() {
    every { webBackendConnectionsHandler.webBackendListConnectionsForWorkspace(any()) } returns WebBackendConnectionReadList()

    val path = "/api/v1/web_backend/connections/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testWebBackendListGeographies() {
    every { webBackendGeographiesHandler.listGeographiesOSS() } returns WebBackendGeographiesListResult()

    val path = "/api/v1/web_backend/geographies/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testWebBackendUpdateConnection() {
    every { webBackendConnectionsHandler.webBackendUpdateConnection(any()) } returns WebBackendConnectionRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/web_backend/connections/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }
}
