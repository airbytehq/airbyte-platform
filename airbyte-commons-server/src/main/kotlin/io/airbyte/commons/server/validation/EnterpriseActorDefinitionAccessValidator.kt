/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.auth.roles.AuthRoleConstants.ADMIN
import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled
import io.airbyte.commons.server.errors.ApplicationErrorKnownException
import io.airbyte.commons.server.handlers.PermissionHandler
import io.airbyte.config.Permission
import io.micronaut.context.annotation.Replaces
import io.micronaut.security.utils.SecurityService
import jakarta.inject.Singleton
import java.util.UUID

/**
 * Enterprise edition implementation of [ActorDefinitionAccessValidator]. Allows any
 * Organization Admin of the default Organization to have write access to any actor definition.
 *
 *
 * NOTE: this class currently doesn't have any special handling for custom actor definitions. We're
 * still evaluating how custom definitions should be handled in Enterprise, and will update this
 * class accordingly once we've made a decision.
 */
@Singleton
@RequiresAirbyteProEnabled
@Replaces(CommunityActorDefinitionAccessValidator::class)
class EnterpriseActorDefinitionAccessValidator(
  private val permissionHandler: PermissionHandler,
  private val securityService: SecurityService,
) : ActorDefinitionAccessValidator {
  @Throws(ApplicationErrorKnownException::class)
  override fun validateWriteAccess(actorDefinitionId: UUID) {
    try {
      val authId = securityService.username().orElse(null)

      // instance admin always has write access
      if (securityService.hasRole(ADMIN)) {
        return
      }

      // In Enterprise, an organization_admin also has write access to all actor definitions, because
      // Enterprise only supports the default organization, and an admin of the org should have write
      // access to all actor definitions within the instance. Note that once actor definition versions
      // are explicitly scoped by organization within the configDb, we can replace this with a more
      // conventional RBAC check via @Secured annotations.
      val defaultOrgPermissionType =
        permissionHandler.findPermissionTypeForUserAndOrganization(DEFAULT_ORGANIZATION_ID, authId)

      if (defaultOrgPermissionType == Permission.PermissionType.ORGANIZATION_ADMIN) {
        return
      }

      // if we haven't returned by now, the user does not have write access.
      throw ApplicationErrorKnownException(
        "User with auth ID $authId does not have write access to actor definition $actorDefinitionId",
      )
    } catch (e: Exception) {
      throw ApplicationErrorKnownException("Could not validate user access to actor definition $actorDefinitionId due to error", e)
    }
  }
}
