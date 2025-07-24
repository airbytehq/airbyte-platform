/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.PermissionCheckRequest
import io.airbyte.api.model.generated.PermissionCreate
import io.airbyte.api.model.generated.PermissionIdRequestBody
import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionReadList
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.api.model.generated.PermissionUpdate
import io.airbyte.api.model.generated.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.api.model.generated.UserIdRequestBody
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Permission
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class PermissionApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var permissionHandler: PermissionHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    permissionHandler = mockk()
    context.registerSingleton(PermissionHandler::class.java, permissionHandler)
  }

  @Test
  fun testCreatePermission() {
    every { permissionHandler.createPermission(any()) } returns
      Permission()
        .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

    val path = "/api/v1/permissions/create"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          PermissionCreate()
            .workspaceId(UUID.randomUUID())
            .permissionType(PermissionType.WORKSPACE_ADMIN),
        ),
      ),
    )
  }

  @Test
  fun testGetPermission() {
    every { permissionHandler.getPermissionRead(any()) } returns PermissionRead()

    val path = "/api/v1/permissions/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionIdRequestBody())))
  }

  @Test
  fun testUpdatePermission() {
    val userId = UUID.randomUUID()
    every { permissionHandler.getPermissionRead(any()) } returns PermissionRead().userId(userId)
    every { permissionHandler.updatePermission(any()) } returns Unit

    val path = "/api/v1/permissions/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionUpdate().permissionId(UUID.randomUUID()))))
  }

  @Test
  fun testDeletePermission() {
    every { permissionHandler.deletePermission(any()) } returns Unit

    val path = "/api/v1/permissions/delete"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionIdRequestBody())))
  }

  @Test
  fun testListPermissionByUser() {
    every { permissionHandler.permissionReadListForUser(any()) } returns PermissionReadList()

    val path = "/api/v1/permissions/list_by_user"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserIdRequestBody().userId(UUID.randomUUID()))))
  }

  @Test
  fun testCheckPermission() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead()

    val path = "/api/v1/permissions/check"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionCheckRequest())))
  }

  @Test
  fun testCheckMultipleWorkspacesPermission() {
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns PermissionCheckRead()

    val path = "/api/v1/permissions/check_multiple_workspaces"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionsCheckMultipleWorkspacesRequest())))
  }
}
