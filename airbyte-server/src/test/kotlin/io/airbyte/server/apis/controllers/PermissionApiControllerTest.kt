/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.PermissionCheckRequest
import io.airbyte.api.server.generated.models.PermissionCreate
import io.airbyte.api.server.generated.models.PermissionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionRead
import io.airbyte.api.server.generated.models.PermissionReadList
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.PermissionUpdate
import io.airbyte.api.server.generated.models.PermissionsCheckMultipleWorkspacesRequest
import io.airbyte.api.server.generated.models.UserIdRequestBody
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
import org.junit.jupiter.api.Assertions.assertEquals
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
        .withPermissionId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withPermissionType(Permission.PermissionType.WORKSPACE_ADMIN)

    val path = "/api/v1/permissions/create"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          PermissionCreate(userId = UUID.randomUUID(), workspaceId = UUID.randomUUID(), permissionType = PermissionType.WORKSPACE_ADMIN),
        ),
      ),
    )
  }

  @Test
  fun testGetPermission() {
    every { permissionHandler.getPermissionRead(any()) } returns
      PermissionRead(permissionId = UUID.randomUUID(), permissionType = PermissionType.WORKSPACE_ADMIN, userId = UUID.randomUUID())

    val path = "/api/v1/permissions/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionIdRequestBody(permissionId = UUID.randomUUID()))))
  }

  @Test
  fun testUpdatePermission() {
    val userId = UUID.randomUUID()
    every { permissionHandler.getPermissionRead(any()) } returns
      PermissionRead(permissionId = UUID.randomUUID(), permissionType = PermissionType.WORKSPACE_ADMIN, userId = userId)
    every { permissionHandler.updatePermission(any()) } returns Unit

    val path = "/api/v1/permissions/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionUpdate(permissionId = UUID.randomUUID()))))
  }

  @Test
  fun testDeletePermission() {
    every { permissionHandler.deletePermission(any()) } returns Unit

    val path = "/api/v1/permissions/delete"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, PermissionIdRequestBody(permissionId = UUID.randomUUID()))))
  }

  @Test
  fun testListPermissionByUser() {
    val userId = UUID.randomUUID()
    val groupId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val permissionId = UUID.randomUUID()
    every { permissionHandler.effectivePermissionReadListForUser(userId) } returns
      PermissionReadList(
        permissions =
          listOf(
            PermissionRead(
              permissionId = permissionId,
              userId = userId,
              groupId = groupId,
              workspaceId = workspaceId,
              permissionType = PermissionType.WORKSPACE_RUNNER,
            ),
          ),
      )

    val response =
      client.toBlocking().retrieve(
        HttpRequest.POST("/api/v1/permissions/list_by_user", UserIdRequestBody(userId = userId)),
        PermissionReadList::class.java,
      )

    assertEquals(1, response.permissions.size)
    val permission = response.permissions.first()
    assertEquals(permissionId, permission.permissionId)
    assertEquals(userId, permission.userId)
    assertEquals(groupId, permission.groupId)
    assertEquals(workspaceId, permission.workspaceId)
    assertEquals(PermissionType.WORKSPACE_RUNNER, permission.permissionType)
  }

  @Test
  fun testCheckPermission() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead(status = PermissionCheckRead.Status.SUCCEEDED)

    val path = "/api/v1/permissions/check"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(path, PermissionCheckRequest(permissionType = PermissionType.WORKSPACE_ADMIN, userId = UUID.randomUUID())),
      ),
    )
  }

  @Test
  fun testCheckMultipleWorkspacesPermission() {
    every { permissionHandler.permissionsCheckMultipleWorkspaces(any()) } returns
      PermissionCheckRead(status = PermissionCheckRead.Status.SUCCEEDED)

    val path = "/api/v1/permissions/check_multiple_workspaces"
    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          path,
          PermissionsCheckMultipleWorkspacesRequest(permissionType = PermissionType.WORKSPACE_ADMIN, userId = UUID.randomUUID()),
        ),
      ),
    )
  }
}
