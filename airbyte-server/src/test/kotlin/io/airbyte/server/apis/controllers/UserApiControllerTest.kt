/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.OrganizationIdRequestBody
import io.airbyte.api.server.generated.models.OrganizationUserReadList
import io.airbyte.api.server.generated.models.UserAuthIdRequestBody
import io.airbyte.api.server.generated.models.UserEmailRequestBody
import io.airbyte.api.server.generated.models.UserGetOrCreateByAuthIdResponse
import io.airbyte.api.server.generated.models.UserIdRequestBody
import io.airbyte.api.server.generated.models.UserRead
import io.airbyte.api.server.generated.models.UserUpdate
import io.airbyte.api.server.generated.models.UserWithPermissionInfoReadList
import io.airbyte.api.server.generated.models.WorkspaceIdRequestBody
import io.airbyte.api.server.generated.models.WorkspaceUserAccessInfoReadList
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.UserHandler
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
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class UserApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var userHandler: UserHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  private fun userRead() = UserRead(userId = UUID.randomUUID(), email = "test@test.com", metadata = emptyMap<String, Any>())

  @BeforeAll
  fun setupMock() {
    userHandler = mockk()
    context.registerSingleton(UserHandler::class.java, userHandler)
  }

  @Test
  fun testGetUser() {
    every { userHandler.getUser(any()) } returns userRead()

    val path = "/api/v1/users/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserIdRequestBody(userId = UUID.randomUUID()))))
  }

  @Test
  fun testGetUserByAuthId() {
    every { userHandler.getUserByAuthId(any()) } returns userRead()

    val path = "/api/v1/users/get_by_auth_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserAuthIdRequestBody(authUserId = "auth-user-id"))))
  }

  @Test
  fun testGetUserByEmail() {
    every { userHandler.getUserByEmail(any()) } returns userRead()

    val path = "/api/v1/users/get_by_email"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserEmailRequestBody(email = "test@test.com"))))
  }

  @Test
  fun testDeleteUser() {
    every { userHandler.deleteUser(any()) } returns Unit

    val path = "/api/v1/users/delete"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserIdRequestBody(userId = UUID.randomUUID()))))
  }

  @Test
  fun testUpdateUser() {
    every { userHandler.updateUser(any()) } returns userRead()

    val path = "/api/v1/users/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserUpdate(userId = UUID.randomUUID()))))
  }

  @Test
  fun testListUsersInOrganization() {
    every { userHandler.listUsersInOrganization(any()) } returns OrganizationUserReadList(users = emptyList())

    val path = "/api/v1/users/list_by_organization_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OrganizationIdRequestBody(organizationId = UUID.randomUUID()))))
  }

  @Test
  fun testListInstanceAdminUsers() {
    every { userHandler.listInstanceAdminUsers() } returns UserWithPermissionInfoReadList(users = emptyList())

    val path = "/api/v1/users/list_instance_admin"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, Jsons.emptyObject())))
  }

  @Test
  fun testGetOrCreateUser() {
    every { userHandler.getOrCreateUserByAuthId(any()) } returns
      UserGetOrCreateByAuthIdResponse(userRead = userRead(), authUserId = "auth-user-id", newUserCreated = false)

    val path = "/api/v1/users/get_or_create_by_auth_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, UserAuthIdRequestBody(authUserId = "auth-user-id"))))
  }

  @Test
  fun testListAccessInfoByWorkspaceId() {
    every { userHandler.listAccessInfoByWorkspaceId(any()) } returns WorkspaceUserAccessInfoReadList(usersWithAccess = emptyList())

    val path = "/api/v1/users/list_access_info_by_workspace_id"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, Jsons.serialize(WorkspaceIdRequestBody(workspaceId = UUID.randomUUID())))))
  }

  @Test
  fun testAgenticEnabledAtSerializesAsIsoString() {
    val agenticEnabledAt = OffsetDateTime.of(2026, 2, 5, 12, 0, 0, 0, ZoneOffset.UTC)
    val userRead =
      UserRead(
        userId = UUID.randomUUID(),
        email = "test@test.com",
        name = "test",
        metadata = emptyMap<String, Any>(),
        agenticEnabledAt = agenticEnabledAt,
      )

    every { userHandler.getUser(any()) } returns userRead

    val path = "/api/v1/users/get"
    val response = client.toBlocking().retrieve(HttpRequest.POST(path, UserIdRequestBody(userId = UUID.randomUUID())), String::class.java)

    // Verify the response contains an ISO-8601 date string, not a numeric timestamp
    Assertions.assertTrue(response.contains("2026-02-05T12:00:00"), "agenticEnabledAt should be serialized as ISO-8601 string, got: $response")
    Assertions.assertFalse(response.contains("\"agenticEnabledAt\":1"), "agenticEnabledAt should not be a numeric timestamp, got: $response")
  }
}
