/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.PermissionCheckRead
import io.airbyte.api.model.generated.SourceDefinitionIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceCreate
import io.airbyte.api.model.generated.WorkspaceCreateWithId
import io.airbyte.api.model.generated.WorkspaceRead
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.server.assertStatus
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
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest(rebuildContext = true)
internal class WorkspaceApiControllerTest {
  @Inject
  lateinit var permissionHandler: PermissionHandler

  @Inject
  lateinit var currentUserService: CurrentUserService

  @Inject
  lateinit var workspacesHandler: WorkspacesHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(PermissionHandler::class)
  fun permissionHandler(): PermissionHandler = mockk()

  @MockBean(CurrentUserService::class)
  fun currentUserService(): CurrentUserService = mockk()

  @MockBean(WorkspacesHandler::class)
  fun workspacesHandler(): WorkspacesHandler = mockk()

  @Test
  fun testCreateWorkspace() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead().status(PermissionCheckRead.StatusEnum.SUCCEEDED) andThen
      PermissionCheckRead().status(PermissionCheckRead.StatusEnum.FAILED)
    every { workspacesHandler.createWorkspace(any()) } returns WorkspaceRead()
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser()

    val path = "/api/v1/workspaces/create"

    // no org id, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    // org id present, permission check succeeds, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceCreate().organizationId(UUID.randomUUID()))))
    // org id present, permission check fails, expect 403
    assertStatus(HttpStatus.FORBIDDEN, client.statusException(HttpRequest.POST(path, WorkspaceCreate().organizationId(UUID.randomUUID()))))
  }

  @Test
  fun testCreateWorkspaceIfNotExist() {
    every { permissionHandler.checkPermissions(any()) } returns PermissionCheckRead().status(PermissionCheckRead.StatusEnum.SUCCEEDED) andThen
      PermissionCheckRead().status(PermissionCheckRead.StatusEnum.FAILED)
    every { workspacesHandler.createWorkspaceIfNotExist(any()) } returns WorkspaceRead()
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser()

    val path = "/api/v1/workspaces/create_if_not_exist"

    // no org id, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceCreateWithId())))
    // org id present, permission check succeeds, expect 200
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceCreateWithId().organizationId(UUID.randomUUID()))))
    // org id present, permission check fails, expect 403
    assertStatus(HttpStatus.FORBIDDEN, client.statusException(HttpRequest.POST(path, WorkspaceCreateWithId().organizationId(UUID.randomUUID()))))
  }

  @Test
  fun testDeleteWorkspace() {
    every { workspacesHandler.deleteWorkspace(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/delete"
    assertStatus(HttpStatus.NO_CONTENT, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetWorkspace() {
    every { workspacesHandler.getWorkspace(any()) } returns WorkspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetBySlugWorkspace() {
    every { workspacesHandler.getWorkspaceBySlug(any()) } returns WorkspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get_by_slug"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testListWorkspace() {
    every { workspacesHandler.listWorkspaces() } returns WorkspaceReadList()

    val path = "/api/v1/workspaces/list"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
  }

  @Test
  fun testUpdateWorkspace() {
    every { workspacesHandler.updateWorkspace(any()) } returns WorkspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testUpdateWorkspaceOrganization() {
    every { workspacesHandler.updateWorkspaceOrganization(any()) } returns WorkspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update_organization"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, WorkspaceUpdateOrganization())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, WorkspaceUpdateOrganization())))
  }

  @Test
  fun testUpdateWorkspaceFeedback() {
    every { workspacesHandler.setFeedbackDone(any()) } returns Unit andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/tag_feedback_status_as_done"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testUpdateWorkspaceName() {
    every { workspacesHandler.updateWorkspaceName(any()) } returns WorkspaceRead() andThenThrows ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/update_name"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceDefinitionIdRequestBody())))
  }

  @Test
  fun testGetWorkspaceByConnectionId() {
    every { workspacesHandler.getWorkspaceByConnectionId(any(), any()) } returns WorkspaceRead() andThenThrows
      ConfigNotFoundException("", "")

    val path = "/api/v1/workspaces/get_by_connection_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SourceIdRequestBody())))
    assertStatus(HttpStatus.NOT_FOUND, client.statusException(HttpRequest.POST(path, SourceIdRequestBody())))
  }
}
