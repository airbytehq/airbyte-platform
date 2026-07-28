/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.authorization

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.auth.TokenType
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as DbAuthProvider

internal class AmbiguousSubjectAuthorizationIntegrationTest : BaseConfigDatabaseTest() {
  private lateinit var userPersistence: UserPersistence
  private lateinit var permissionHandler: PermissionHandler
  private lateinit var roleResolver: RoleResolver

  @BeforeEach
  fun setUp() {
    truncateAllTables()
    userPersistence = UserPersistence(database!!)
    permissionHandler = mockk()
    roleResolver =
      RoleResolver(
        AuthenticationHeaderResolver(
          mockk<WorkspaceHelper>(),
          permissionHandler,
          userPersistence,
          mockk<DataplaneGroupService>(),
          mockk<DataplaneService>(),
        ),
        mockk<CurrentUserService>(),
        mockk<SecurityService>(),
        permissionHandler,
      )
  }

  @Test
  fun `ambiguous raw subject is not SELF for either target User`() {
    val firstUserId = UUID.randomUUID()
    val secondUserId = UUID.randomUUID()
    val sharedSubject = "shared-authorization-subject"
    writeUser(firstUserId, sharedSubject, AuthProvider.KEYCLOAK)
    writeUser(secondUserId, "second-unique-subject", AuthProvider.KEYCLOAK)
    database!!.query { ctx ->
      ctx
        .insertInto(Tables.AUTH_USER)
        .set(Tables.AUTH_USER.ID, UUID.randomUUID())
        .set(Tables.AUTH_USER.USER_ID, secondUserId)
        .set(Tables.AUTH_USER.AUTH_USER_ID, sharedSubject)
        .set(Tables.AUTH_USER.AUTH_PROVIDER, DbAuthProvider.google_identity_platform)
        .execute()
    }
    every { permissionHandler.getPermissionsByAuthUserId(sharedSubject) } returns emptyList()

    listOf(firstUserId, secondUserId).forEach { targetUserId ->
      val roles =
        roleResolver
          .newRequest()
          .withSubject(sharedSubject, TokenType.USER)
          .withRef(AuthenticationId.AIRBYTE_USER_ID, targetUserId)
          .roles()

      assertThat(roles).doesNotContain(AuthRoleConstants.SELF)
    }
  }

  @Test
  fun `same-owner multi-provider raw subject remains SELF`() {
    val userId = UUID.randomUUID()
    val sharedSubject = "same-owner-multi-provider-subject"
    writeUser(userId, sharedSubject, AuthProvider.KEYCLOAK)
    database!!.query { ctx ->
      ctx
        .insertInto(Tables.AUTH_USER)
        .set(Tables.AUTH_USER.ID, UUID.randomUUID())
        .set(Tables.AUTH_USER.USER_ID, userId)
        .set(Tables.AUTH_USER.AUTH_USER_ID, sharedSubject)
        .set(Tables.AUTH_USER.AUTH_PROVIDER, DbAuthProvider.google_identity_platform)
        .execute()
    }
    every { permissionHandler.getPermissionsByAuthUserId(sharedSubject) } returns emptyList()

    val roles =
      roleResolver
        .newRequest()
        .withSubject(sharedSubject, TokenType.USER)
        .withRef(AuthenticationId.AIRBYTE_USER_ID, userId)
        .roles()

    assertThat(roles).contains(AuthRoleConstants.SELF)
  }

  private fun writeUser(
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider,
  ) {
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(userId)
        .withEmail("$userId@example.com")
        .withName("Authorization Test User")
        .withAuthUserId(authUserId)
        .withAuthProvider(authProvider),
    )
  }
}
