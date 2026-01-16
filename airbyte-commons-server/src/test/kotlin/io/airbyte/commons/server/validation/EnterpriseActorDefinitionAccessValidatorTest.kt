/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Permission
import io.micronaut.security.utils.SecurityService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

internal class EnterpriseActorDefinitionAccessValidatorTest {
  private lateinit var mSecurityService: SecurityService
  private lateinit var permissionHandler: PermissionHandler

  private lateinit var enterpriseActorDefinitionAccessValidator: EnterpriseActorDefinitionAccessValidator

  @BeforeEach
  fun setup() {
    mSecurityService = mockk(relaxed = true)
    permissionHandler = mockk(relaxed = true)
    // Default stub for username to avoid ClassCastException with relaxed mocks
    every { mSecurityService.username() } returns Optional.empty()
    every { mSecurityService.hasRole(any()) } returns false
    enterpriseActorDefinitionAccessValidator = EnterpriseActorDefinitionAccessValidator(permissionHandler, mSecurityService)
  }

  @Nested
  internal inner class ValidateWriteAccess {
    @Test
    fun instanceAdminAllowed() {
      every { mSecurityService.hasRole(ADMIN) } returns true

      // any actor definition ID passes this check for an instance admin.
      Assertions.assertDoesNotThrow { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) }
    }

    @Test
    fun defaultOrgAdminAllowed() {
      every { mSecurityService.username() } returns Optional.of(USERNAME)
      every {
        permissionHandler.findPermissionTypeForUserAndOrganization(
          DEFAULT_ORGANIZATION_ID,
          USERNAME,
        )
      } returns Permission.PermissionType.ORGANIZATION_ADMIN

      // an org admin of the instance's default org should have write access to any actor definition.
      Assertions.assertDoesNotThrow { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) }
    }
  }

  @Test
  fun otherwiseThrows() {
    every { mSecurityService.username() } returns Optional.of(USERNAME)
    every { mSecurityService.hasRole(ADMIN) } returns false
    every {
      permissionHandler.findPermissionTypeForUserAndOrganization(DEFAULT_ORGANIZATION_ID, USERNAME)
    } returns Permission.PermissionType.ORGANIZATION_EDITOR

    // any other permission type should throw an exception.
    Assertions.assertThrows(
      ApplicationErrorKnownException::class.java,
    ) { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) }
  }

  companion object {
    private const val USERNAME = "user"
  }
}
