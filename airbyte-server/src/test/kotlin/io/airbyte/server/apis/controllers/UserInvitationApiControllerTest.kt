/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.models.InviteCodeRequestBody
import io.airbyte.api.server.generated.models.PermissionType
import io.airbyte.api.server.generated.models.ScopeType
import io.airbyte.api.server.generated.models.UserInvitationAdminRead
import io.airbyte.api.server.generated.models.UserInvitationCancelRequestBody
import io.airbyte.api.server.generated.models.UserInvitationCreateRequestBody
import io.airbyte.api.server.generated.models.UserInvitationCreateResponse
import io.airbyte.api.server.generated.models.UserInvitationRead
import io.airbyte.api.server.generated.models.UserInvitationStatus
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.server.assertStatus
import io.airbyte.server.handlers.UserInvitationHandler
import io.airbyte.server.helpers.UserInvitationAuthorizationHelper
import io.airbyte.server.status
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class UserInvitationApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var userInvitationHandler: UserInvitationHandler

  lateinit var userInvitationAuthorizationHelper: UserInvitationAuthorizationHelper

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  private val invitationId = UUID.randomUUID()

  @BeforeAll
  fun setupMock() {
    userInvitationHandler = mockk()
    userInvitationAuthorizationHelper = mockk()
    val currentUserService = mockk<CurrentUserService>()
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID()).withEmail("user@airbyte.io")
    context.registerSingleton(UserInvitationHandler::class.java, userInvitationHandler)
    context.registerSingleton(UserInvitationAuthorizationHelper::class.java, userInvitationAuthorizationHelper)
    context.registerSingleton(CurrentUserService::class.java, currentUserService)
  }

  @Test
  fun testCreateUserInvitation() {
    every { userInvitationHandler.createInvitationOrPermission(any(), any()) } returns UserInvitationCreateResponse(directlyAdded = true)

    assertStatus(
      HttpStatus.OK,
      client.status(
        HttpRequest.POST(
          "/api/v1/user_invitations/create",
          UserInvitationCreateRequestBody(
            invitedEmail = "invitee@airbyte.io",
            permissionType = PermissionType.WORKSPACE_ADMIN,
            scopeType = ScopeType.WORKSPACE,
            scopeId = UUID.randomUUID(),
          ),
        ),
      ),
    )
  }

  @Test
  fun testAcceptUserInvitation() {
    every { userInvitationHandler.accept(any(), any()) } returns
      UserInvitationRead(
        id = invitationId,
        inviteCode = "code",
        inviterUserId = UUID.randomUUID(),
        invitedEmail = "invitee@airbyte.io",
        scopeId = UUID.randomUUID(),
        scopeType = ScopeType.WORKSPACE,
        permissionType = PermissionType.WORKSPACE_ADMIN,
        status = UserInvitationStatus.ACCEPTED,
        createdAt = 0,
        updatedAt = 0,
      )

    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST("/api/v1/user_invitations/accept", InviteCodeRequestBody(inviteCode = "code"))),
    )
  }

  @Test
  fun testDeclineUserInvitation() {
    // decline is not implemented, so a routed request fails with a 500 rather than a 404
    assertStatus(
      HttpStatus.INTERNAL_SERVER_ERROR,
      client.statusException(HttpRequest.POST("/api/v1/user_invitations/decline", InviteCodeRequestBody(inviteCode = "code"))),
    )
  }

  @Test
  fun testCancelUserInvitation() {
    every { userInvitationAuthorizationHelper.authorizeInvitationAdmin(invitationId, any<UUID>()) } just runs
    every { userInvitationHandler.cancel(any()) } returns
      UserInvitationAdminRead(
        id = invitationId,
        inviterUserId = UUID.randomUUID(),
        invitedEmail = "invitee@airbyte.io",
        scopeId = UUID.randomUUID(),
        scopeType = ScopeType.WORKSPACE,
        permissionType = PermissionType.WORKSPACE_ADMIN,
        status = UserInvitationStatus.CANCELLED,
        createdAt = 0,
        updatedAt = 0,
      )

    assertStatus(
      HttpStatus.OK,
      client.status(HttpRequest.POST("/api/v1/user_invitations/cancel", UserInvitationCancelRequestBody(id = invitationId))),
    )
  }
}
