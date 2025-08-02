/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants.ADMIN
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Permission
import io.micronaut.security.utils.SecurityService
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.function.Executable
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.jupiter.MockitoExtension
import java.io.IOException
import java.util.Optional
import java.util.UUID

@ExtendWith(MockitoExtension::class)
internal class EnterpriseActorDefinitionAccessValidatorTest {
  @Mock
  private lateinit var mSecurityService: SecurityService

  @Mock
  private lateinit var permissionHandler: PermissionHandler

  private lateinit var enterpriseActorDefinitionAccessValidator: EnterpriseActorDefinitionAccessValidator

  @BeforeEach
  fun setup() {
    enterpriseActorDefinitionAccessValidator = EnterpriseActorDefinitionAccessValidator(permissionHandler, mSecurityService)
  }

  @Nested
  internal inner class ValidateWriteAccess {
    @Test
    fun instanceAdminAllowed() {
      Mockito.`when`(mSecurityService.hasRole(ADMIN)).thenReturn(true)

      // any actor definition ID passes this check for an instance admin.
      Assertions.assertDoesNotThrow(Executable { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) })
    }

    @Test
    @Throws(IOException::class)
    fun defaultOrgAdminAllowed() {
      Mockito.`when`(mSecurityService.username()).thenReturn(Optional.of(USERNAME))
      Mockito
        .`when`(
          permissionHandler.findPermissionTypeForUserAndOrganization(
            DEFAULT_ORGANIZATION_ID,
            USERNAME,
          ),
        ).thenReturn(Permission.PermissionType.ORGANIZATION_ADMIN)

      // an org admin of the instance's default org should have write access to any actor definition.
      Assertions.assertDoesNotThrow(Executable { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) })
    }
  }

  @Test
  @Throws(IOException::class)
  fun otherwiseThrows() {
    Mockito.`when`(mSecurityService!!.username()).thenReturn(Optional.of(USERNAME))
    Mockito.`when`(mSecurityService.hasRole(ADMIN)).thenReturn(false)
    Mockito
      .`when`(permissionHandler.findPermissionTypeForUserAndOrganization(DEFAULT_ORGANIZATION_ID, USERNAME))
      .thenReturn(Permission.PermissionType.ORGANIZATION_EDITOR)

    // any other permission type should throw an exception.
    Assertions.assertThrows(
      ApplicationErrorKnownException::class.java,
    ) { enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()) }
  }

  companion object {
    private const val USERNAME = "user"
  }
}
