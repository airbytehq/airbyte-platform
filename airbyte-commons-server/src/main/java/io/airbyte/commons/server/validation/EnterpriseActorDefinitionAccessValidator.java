/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation;

import io.airbyte.commons.license.annotation.RequiresAirbyteProEnabled;
import io.airbyte.commons.server.errors.ApplicationErrorKnownException;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.security.utils.SecurityService;
import jakarta.inject.Singleton;
import java.util.UUID;

/**
 * Enterprise edition implementation of {@link ActorDefinitionAccessValidator}. Allows any
 * Organization Admin of the default Organization to have write access to any actor definition.
 * <p>
 * NOTE: this class currently doesn't have any special handling for custom actor definitions. We're
 * still evaluating how custom definitions should be handled in Enterprise, and will update this
 * class accordingly once we've made a decision.
 */
@Singleton
@RequiresAirbyteProEnabled
@Replaces(CommunityActorDefinitionAccessValidator.class)
public class EnterpriseActorDefinitionAccessValidator implements ActorDefinitionAccessValidator {

  private final PermissionPersistence permissionPersistence;
  private final SecurityService securityService;

  public EnterpriseActorDefinitionAccessValidator(final PermissionPersistence permissionPersistence,
                                                  final SecurityService securityService) {
    this.permissionPersistence = permissionPersistence;
    this.securityService = securityService;
  }

  @Override
  public void validateWriteAccess(final UUID actorDefinitionId) throws ApplicationErrorKnownException {
    try {
      final String authId = securityService.username().orElse(null);

      // instance admin always has write access
      if (permissionPersistence.isAuthUserInstanceAdmin(authId)) {
        return;
      }

      // In Enterprise, an organization_admin also has write access to all actor definitions, because
      // Enterprise only supports the default organization, and an admin of the org should have write
      // access to all actor definitions within the instance. Note that once actor definition versions
      // are explicitly scoped by organization within the configDb, we can replace this with a more
      // conventional RBAC check via @Secured annotations.
      final PermissionType defaultOrgPermissionType =
          permissionPersistence.findPermissionTypeForUserAndOrganization(OrganizationPersistence.DEFAULT_ORGANIZATION_ID, authId);

      if (defaultOrgPermissionType.equals(PermissionType.ORGANIZATION_ADMIN)) {
        return;
      }

      // if we haven't returned by now, the user does not have write access.
      throw new ApplicationErrorKnownException(
          "User with auth ID " + authId + " does not have write access to actor definition " + actorDefinitionId);
    } catch (final Exception e) {
      throw new ApplicationErrorKnownException("Could not validate user access to actor definition " + actorDefinitionId + " due to error", e);
    }
  }

}
