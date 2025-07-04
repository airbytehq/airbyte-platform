/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.commons.auth.roles.AuthRoleConstants.ADMIN;
import static org.mockito.Mockito.when;

import io.airbyte.commons.server.errors.ApplicationErrorKnownException;
import io.airbyte.commons.server.handlers.PermissionHandler;
import io.airbyte.config.Permission.PermissionType;
import io.micronaut.security.utils.SecurityService;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EnterpriseActorDefinitionAccessValidatorTest {

  private static final String USERNAME = "user";

  @Mock
  private SecurityService mSecurityService;
  @Mock
  private PermissionHandler permissionHandler;

  private EnterpriseActorDefinitionAccessValidator enterpriseActorDefinitionAccessValidator;

  @BeforeEach
  void setup() {
    enterpriseActorDefinitionAccessValidator = new EnterpriseActorDefinitionAccessValidator(permissionHandler, mSecurityService);
  }

  @Nested
  class ValidateWriteAccess {

    @Test
    void instanceAdminAllowed() {
      when(mSecurityService.hasRole(ADMIN)).thenReturn(true);

      // any actor definition ID passes this check for an instance admin.
      Assertions.assertDoesNotThrow(() -> enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()));
    }

    @Test
    void defaultOrgAdminAllowed() throws IOException {
      when(mSecurityService.username()).thenReturn(java.util.Optional.of(USERNAME));
      when(permissionHandler.findPermissionTypeForUserAndOrganization(DEFAULT_ORGANIZATION_ID, USERNAME))
          .thenReturn(PermissionType.ORGANIZATION_ADMIN);

      // an org admin of the instance's default org should have write access to any actor definition.
      Assertions.assertDoesNotThrow(() -> enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()));
    }

  }

  @Test
  void otherwiseThrows() throws IOException {
    when(mSecurityService.username()).thenReturn(java.util.Optional.of(USERNAME));
    when(mSecurityService.hasRole(ADMIN)).thenReturn(false);
    when(permissionHandler.findPermissionTypeForUserAndOrganization(DEFAULT_ORGANIZATION_ID, USERNAME))
        .thenReturn(PermissionType.ORGANIZATION_EDITOR);

    // any other permission type should throw an exception.
    Assertions.assertThrows(ApplicationErrorKnownException.class,
        () -> enterpriseActorDefinitionAccessValidator.validateWriteAccess(UUID.randomUUID()));
  }

}
