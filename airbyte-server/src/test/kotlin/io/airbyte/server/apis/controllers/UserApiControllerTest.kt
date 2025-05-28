/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationUserReadList
import io.airbyte.api.model.generated.UserAuthIdRequestBody
import io.airbyte.api.model.generated.UserEmailRequestBody
import io.airbyte.api.model.generated.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.model.generated.UserIdRequestBody
import io.airbyte.api.model.generated.UserRead
import io.airbyte.api.model.generated.UserUpdate
import io.airbyte.api.model.generated.UserWithPermissionInfoReadList
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.model.generated.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
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

@MicronautTest
internal class UserApiControllerTest {
  @Inject
  lateinit var userHandler: UserHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(UserHandler::class)
  fun userHandler(): UserHandler = mockk()

  @Test
  fun testGetUser() {
    every { userHandler.getUser(any()) } returns UserRead()

    val path = "/api/v1/users/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserIdRequestBody())))
  }

  @Test
  fun testGetUserByAuthId() {
    every { userHandler.getUserByAuthId(any()) } returns UserRead()

    val path = "/api/v1/users/get_by_auth_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserAuthIdRequestBody())))
  }

  @Test
  fun testGetUserByEmail() {
    every { userHandler.getUserByEmail(any()) } returns UserRead()

    val path = "/api/v1/users/get_by_email"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserEmailRequestBody())))
  }

  @Test
  fun testDeleteUser() {
    every { userHandler.deleteUser(any()) } returns Unit

    val path = "/api/v1/users/delete"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserIdRequestBody())))
  }

  @Test
  fun testUpdateUser() {
    every { userHandler.updateUser(any()) } returns UserRead()

    val path = "/api/v1/users/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserUpdate())))
  }

  @Test
  fun testListUsersInOrganization() {
    every { userHandler.listUsersInOrganization(any()) } returns OrganizationUserReadList()

    val path = "/api/v1/users/list_by_organization_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OrganizationIdRequestBody())))
  }

  @Test
  fun testListInstanceAdminUsers() {
    every { userHandler.listInstanceAdminUsers() } returns UserWithPermissionInfoReadList()

    val path = "/api/v1/users/list_instance_admin"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, Jsons.emptyObject())))
  }

  @Test
  fun testGetOrCreateUser() {
    every { userHandler.getOrCreateUserByAuthId(any()) } returns UserGetOrCreateByAuthIdResponse().userRead(UserRead())

    val path = "/api/v1/users/get_or_create_by_auth_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserAuthIdRequestBody())))
  }

  @Test
  fun testListAccessInfoByWorkspaceId() {
    every { userHandler.listAccessInfoByWorkspaceId(any()) } returns WorkspaceUserAccessInfoReadList()

    val path = "/api/v1/users/list_access_info_by_workspace_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, Jsons.serialize(WorkspaceIdRequestBody()))))
  }
}
