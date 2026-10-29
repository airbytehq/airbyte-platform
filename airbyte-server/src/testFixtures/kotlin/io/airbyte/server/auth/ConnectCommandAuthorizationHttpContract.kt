/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.logging.LogUtils
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.server.apis.controllers.CommandApiController
import io.airbyte.server.services.CommandService
import io.micronaut.runtime.server.EmbeddedServer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

/** The same production route and rule are exercised on OSS and Cloud classpaths. */
abstract class ConnectCommandAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `runner and admin permissions can run a connect test`() {
    for (permission in listOf(Permission.PermissionType.WORKSPACE_RUNNER, Permission.PermissionType.WORKSPACE_ADMIN)) {
      request(workspaceId = WORKSPACE, expectedStatus = 200, permission = permission)
    }
  }

  @Test
  fun `reader permission is denied`() {
    request(workspaceId = WORKSPACE, expectedStatus = 403, permission = Permission.PermissionType.WORKSPACE_READER)
  }

  @Test
  fun `no permission on the workspace is denied`() {
    request(workspaceId = WORKSPACE, expectedStatus = 403, permission = null)
  }

  @Test
  fun `permission on another workspace does not grant access`() {
    request(workspaceId = OTHER, expectedStatus = 403, permission = Permission.PermissionType.WORKSPACE_ADMIN)
  }

  @Test
  fun `workspace scoped tokens only reach their own workspace`() {
    val bothWorkspaces = listOf(WORKSPACE, OTHER)
    request(workspaceId = WORKSPACE, expectedStatus = 200, permittedWorkspaces = bothWorkspaces, token = "scoped")
    request(workspaceId = OTHER, expectedStatus = 403, permittedWorkspaces = bothWorkspaces, token = "scoped")
    request(workspaceId = WORKSPACE, expectedStatus = 403, permittedWorkspaces = bothWorkspaces, token = "other-scope")
  }

  @Test
  fun `authentication is required`() {
    request(workspaceId = WORKSPACE, expectedStatus = 401, token = "")
  }

  @Test
  fun `malformed input is rejected before any work`() {
    for (fields in listOf(
      """"id":"","workspace_id":"$WORKSPACE","host":"example.com","port":5432""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"","port":5432""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"   ","port":5432""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"999.0.0.1","port":5432""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"1:2:3:4:5:6:7:8:9","port":5432""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"example.com","port":0""",
      """"id":"cmd","workspace_id":"$WORKSPACE","host":"example.com","port":65536""",
      """"id":"cmd","workspace_id":"$WORKSPACE","port":5432""",
      """"id":"cmd","host":"example.com","port":5432""",
    )) {
      request(rawBody = "{$fields}", expectedStatus = 400)
    }
  }

  private fun request(
    expectedStatus: Int,
    workspaceId: UUID = WORKSPACE,
    permission: Permission.PermissionType? = Permission.PermissionType.WORKSPACE_ADMIN,
    permittedWorkspaces: List<UUID> = listOf(WORKSPACE),
    token: String = "valid",
    rawBody: String = """{"id":"connect-command","workspace_id":"$workspaceId","host":"example.com","port":5432}""",
  ) {
    checkCommandContext(cloud, security = true).use { context ->
      val workspaces = mockk<WorkspaceService>()
      for (id in listOf(WORKSPACE, OTHER)) {
        every { workspaces.getStandardWorkspaceNoSecrets(id, false) } returns StandardWorkspace().withWorkspaceId(id).withOrganizationId(ORGANIZATION)
      }
      val permissions = mockk<PermissionService>()
      every { permissions.getPermissionsByAuthUserId(any()) } returns
        listOfNotNull(permission).flatMap { type -> permittedWorkspaces.map { Permission().withWorkspaceId(it).withPermissionType(type) } }
      every { permissions.getPermissionsByServiceAccountId(any()) } returns emptyList()
      val data =
        AuthorizationDataService(
          workspaces,
          permissions,
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          mockk(),
          privateLinks = mockk(),
          connectionEvents = mockk(),
          actorRepository = mockk(),
          attempts = mockk(),
          entitlements = mockk(),
          featureFlags = mockk(),
        )
      val commands = mockk<CommandService>()
      every { commands.createConnectCommand(any(), any(), any(), any()) } returns true
      val operationPermissions = OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true)
      context.registerSingleton(AuthorizationDataService::class.java, data)
      context.registerSingleton(OperationPermissions::class.java, operationPermissions)
      context.registerSingleton(ConnectCommandRule::class.java, ConnectCommandRule(data, operationPermissions))
      context.registerSingleton(CommandService::class.java, commands)
      context.registerSingleton(RoleResolver::class.java, mockk<RoleResolver>(relaxed = true))
      context.registerSingleton(ActorRepository::class.java, mockk<ActorRepository>())
      context.registerSingleton(WorkspaceService::class.java, workspaces)
      context.registerSingleton(CatalogConverter::class.java, mockk<CatalogConverter>())
      context.registerSingleton(ApiPojoConverters::class.java, mockk<ApiPojoConverters>())
      context.registerSingleton(LogUtils::class.java, mockk<LogUtils>())
      registerSiblingOperationRules(context, CommandApiController::class.java, setOf(ConnectCommandRule::class.java))
      context.start()
      context.getBean(EmbeddedServer::class.java).start().use { server ->
        HttpClient.newHttpClient().use { client ->
          val builder =
            HttpRequest
              .newBuilder(URI("${server.url}/api/v1/commands/run/connect"))
              .header("Content-Type", "application/json")
          if (token.isNotEmpty()) builder.header("Authorization", "Bearer $token")
          val response = client.send(builder.POST(HttpRequest.BodyPublishers.ofString(rawBody)).build(), HttpResponse.BodyHandlers.ofString())

          assertEquals(expectedStatus, response.statusCode(), response.body())
          if (expectedStatus == 200) {
            verify(exactly = 1) { commands.createConnectCommand("connect-command", workspaceId, "example.com", 5432) }
          } else {
            verify(exactly = 0) { commands.createConnectCommand(any(), any(), any(), any()) }
          }
        }
      }
    }
  }

  companion object {
    val WORKSPACE: UUID = UUID.fromString("00000000-0000-0000-0000-000000000003")
    val OTHER: UUID = UUID.fromString("00000000-0000-0000-0000-000000000004")
    val ORGANIZATION: UUID = UUID.fromString("00000000-0000-0000-0000-000000000006")
  }
}
