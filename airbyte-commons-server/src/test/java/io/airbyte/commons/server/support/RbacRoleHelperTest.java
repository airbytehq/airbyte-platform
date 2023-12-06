/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.support;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.airbyte.commons.auth.AuthRole;
import io.airbyte.commons.auth.AuthRoleConstants;
import io.airbyte.commons.auth.OrganizationAuthRole;
import io.airbyte.commons.auth.WorkspaceAuthRole;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.persistence.PermissionPersistence;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.netty.NettyHttpHeaders;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RbacRoleHelperTest {

  private static final String AUTH_USER_ID = UUID.randomUUID().toString();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();

  @Mock
  private AuthenticationHeaderResolver mHeaderResolver;
  @Mock
  private PermissionPersistence mPermissionPersistence;
  @Mock
  private HttpRequest mRequest;
  @Mock
  private NettyHttpHeaders mHeaders;

  private RbacRoleHelper rbacRoleHelper;

  @BeforeEach
  void setUp() {
    rbacRoleHelper = new RbacRoleHelper(mHeaderResolver, mPermissionPersistence);
    when(mRequest.getHeaders()).thenReturn(mHeaders);
  }

  @ParameterizedTest
  @ArgumentsSource(GetRbacRolesArgumentsProvider.class)
  void getRbacRoles(final Boolean hasWorkspacePermission,
                    final Boolean hasOrganizationPermission,
                    final Boolean userMatchesTargetUser,
                    final Boolean isInstanceAdmin)
      throws IOException {

    final Set<String> expectedRoles = new HashSet<>();

    if (hasWorkspacePermission) {
      when(mHeaderResolver.resolveWorkspace(any())).thenReturn(List.of(WORKSPACE_ID));
      when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(WORKSPACE_ID, AUTH_USER_ID))
          .thenReturn(PermissionType.WORKSPACE_ADMIN);

      // expect all workspace roles that are admin and below
      expectedRoles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN));
    }
    if (hasOrganizationPermission) {
      when(mHeaderResolver.resolveOrganization(any())).thenReturn(List.of(ORGANIZATION_ID));
      when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(ORGANIZATION_ID, AUTH_USER_ID))
          .thenReturn(PermissionType.ORGANIZATION_EDITOR);

      // expect all org roles that are editor and below
      expectedRoles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_EDITOR));
    }
    if (userMatchesTargetUser) {
      when(mHeaderResolver.resolveUserAuthId(any())).thenReturn(AUTH_USER_ID);

      // expect roles to contain the SELF role
      expectedRoles.add(AuthRoleConstants.SELF);
    }
    if (isInstanceAdmin) {
      when(mPermissionPersistence.isAuthUserInstanceAdmin(AUTH_USER_ID)).thenReturn(true);

      // expect all roles that are admin and below
      expectedRoles.addAll(AuthRole.buildAuthRolesSet(AuthRole.ADMIN));
      expectedRoles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_ADMIN));
      expectedRoles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_ADMIN));
    }

    final Collection<String> actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));

    Assertions.assertEquals(expectedRoles, actualRoles);
  }

  static class GetRbacRolesArgumentsProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      final List<Boolean> hasWorkspacePermissionArgs = List.of(true, false);
      final List<Boolean> hasOrganizationPermissionArgs = List.of(true, false);
      final List<Boolean> userMatchesTargetUserArgs = List.of(true, false);
      final List<Boolean> isInstanceAdminArgs = List.of(true, false);

      // return all permutations of the above lists as an arguments stream
      return hasWorkspacePermissionArgs.stream()
          .flatMap(hasWorkspacePermission -> hasOrganizationPermissionArgs.stream()
              .flatMap(hasOrganizationPermission -> userMatchesTargetUserArgs.stream()
                  .flatMap(userMatchesTargetUser -> isInstanceAdminArgs.stream()
                      .map(isInstanceAdmin -> Arguments.of(hasWorkspacePermission, hasOrganizationPermission, userMatchesTargetUser,
                          isInstanceAdmin)))));

    }

  }

}
