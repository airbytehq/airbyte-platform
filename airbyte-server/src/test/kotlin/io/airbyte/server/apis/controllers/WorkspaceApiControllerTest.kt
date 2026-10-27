/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.PermissionCheckRead
import io.airbyte.api.server.generated.models.SlugRequestBody
import io.airbyte.api.server.generated.models.WorkspaceCreate
import io.airbyte.api.server.generated.models.WorkspaceCreateWithId
import io.airbyte.api.server.generated.models.WorkspaceGiveFeedback
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceRead
import io.airbyte.api.server.generated.models.WorkspaceUpdate
import io.airbyte.api.server.generated.models.WorkspaceUpdateName
import io.airbyte.api.server.generated.models.WorkspaceUpdateOrganization
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
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
@Property(name = "controller.response.authorization.fixture", value = "true")
internal class WorkspaceApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var permissionHandler: PermissionHandler
  lateinit var currentUserService: CurrentUserService
  lateinit var workspacesHandler: WorkspacesHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    permissionHandler = mockk()
    context.registerSingleton(PermissionHandler::class.java, permissionHandler)
    currentUserService = mockk()
    context.registerSingleton(CurrentUserService::class.java, currentUserService)
    workspacesHandler = mockk()
    context.registerSingleton(WorkspacesHandler::class.java, workspacesHandler)
  }

  @Test
  fun testCreateWorkspace() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead(status = PermissionCheckRead.Status.SUCCEEDED) andThen
      PermissionCheckRead(status = PermissionCheckRead.Status.FAILED)
    every { workspacesHandler.createWorkspace(any()) } returns workspaceRead()
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID())

    val path = "/api/v1/workspaces/create"

    // no org id, expect 400
    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, mapOf("name" to "name"))))
    // org id present, permission check succeeds, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceCreate(name = "name", organizationId = UUID.randomUUID()))))
    // org id present, permission check fails, expect 403
    assertStatus(
      HttpStatus.FORBIDDEN,
      client.statusException(HttpRequest.POST(path, WorkspaceCreate(name = "name", organizationId = UUID.randomUUID()))),
    )
  }

  @Test
  fun testCreateWorkspaceIfNotExist() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead(status = PermissionCheckRead.Status.SUCCEEDED) andThen
      PermissionCheckRead(status = PermissionCheckRead.Status.FAILED)
    every { workspacesHandler.createWorkspaceIfNotExist(any()) } returns workspaceRead()
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID())

    val path = "/api/v1/workspaces/create_if_not_exist"

    // no org id, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceCreateWithId(id = UUID.randomUUID(), name = "name"))))
    // org id present, permission check succeeds, expect 200
    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST(path, WorkspaceCreateWithId(id = UUID.randomUUID(), name = "name", organizationId = UUID.randomUUID()))),
    )
    // org id present, permission check fails, expect 403
    assertStatus(
      HttpStatus.FORBIDDEN,
      client.statusException(
        HttpRequest.POST(path, WorkspaceCreateWithId(id = UUID.randomUUID(), name = "name", organizationId = UUID.randomUUID())),
      ),
    )
  }

  @Test
  fun testDeleteWorkspace() {
    every { workspacesHandler.deleteWorkspace(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody(workspaceId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody(workspaceId = UUID.randomUUID()))))
  }

  @Test
  fun testGetWorkspace() {
    every { workspacesHandler.getWorkspace(any()) } returns workspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceIdRequestBody(workspaceId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceIdRequestBody(workspaceId = UUID.randomUUID()))))
  }

  @Test
  fun testGetBySlugWorkspace() {
    every { workspacesHandler.getWorkspaceBySlug(any()) } returns workspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get_by_slug"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SlugRequestBody(slug = "slug"))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SlugRequestBody(slug = "slug"))))
  }

  @Test
  fun testUpdateWorkspace() {
    every { workspacesHandler.updateWorkspace(any()) } returns workspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceUpdate(workspaceId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceUpdate(workspaceId = UUID.randomUUID()))))
  }

  @Test
  fun testUpdateWorkspaceOrganization() {
    every { workspacesHandler.updateWorkspaceOrganization(any()) } returns workspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update_organization"
    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST(path, WorkspaceUpdateOrganization(workspaceId = UUID.randomUUID(), organizationId = UUID.randomUUID()))),
    )
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(
        HttpRequest.POST(path, WorkspaceUpdateOrganization(workspaceId = UUID.randomUUID(), organizationId = UUID.randomUUID())),
      ),
    )
  }

  @Test
  fun testUpdateWorkspaceFeedback() {
    every { workspacesHandler.setFeedbackDone(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/tag_feedback_status_as_done"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceGiveFeedback(workspaceId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceGiveFeedback(workspaceId = UUID.randomUUID()))))
  }

  @Test
  fun testUpdateWorkspaceName() {
    every { workspacesHandler.updateWorkspaceName(any()) } returns workspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update_name"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceUpdateName(workspaceId = UUID.randomUUID(), name = "name"))))
    assertStatus(
      HttpStatus.NOT_FOUND,
      client.statusException(HttpRequest.POST(path, WorkspaceUpdateName(workspaceId = UUID.randomUUID(), name = "name"))),
    )
  }

  @Test
  fun testGetWorkspaceByConnectionId() {
    every { workspacesHandler.getWorkspaceByConnectionId(any(), any()) } returns workspaceRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get_by_connection_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, ConnectionIdRequestBody(connectionId = UUID.randomUUID()))))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, ConnectionIdRequestBody(connectionId = UUID.randomUUID()))))
  }

  private fun workspaceRead() =
    WorkspaceRead(
      workspaceId = UUID.randomUUID(),
      customerId = UUID.randomUUID(),
      name = "name",
      slug = "slug",
      initialSetupComplete = true,
      organizationId = UUID.randomUUID(),
    )
}
