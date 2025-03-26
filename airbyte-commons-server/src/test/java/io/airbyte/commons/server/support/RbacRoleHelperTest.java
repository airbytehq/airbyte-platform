/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.Mockito;
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
    Mockito.lenient().when(mRequest.getHeaders()).thenReturn(mHeaders);
  }

  @ParameterizedTest
  @ArgumentsSource(GetRbacRolesArgumentsProvider.class)
  void getRbacRolesBasic(final Boolean hasWorkspacePermission,
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
      // org editor role also grants workspace editor and workspace reader roles
      expectedRoles.add(PermissionType.WORKSPACE_READER.name());
      expectedRoles.add(PermissionType.WORKSPACE_RUNNER.name());
      expectedRoles.add(PermissionType.WORKSPACE_EDITOR.name());
    }
    if (userMatchesTargetUser) {
      when(mHeaderResolver.resolveAuthUserIds(any())).thenReturn(Set.of(AUTH_USER_ID, UUID.randomUUID().toString()));

      // expect roles to contain the SELF role
      expectedRoles.add(AuthRoleConstants.SELF);
    } else {
      // return two random auth user ids that do not match the auth user id, do not expect a SELF role
      when(mHeaderResolver.resolveAuthUserIds(any())).thenReturn(Set.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
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

  @Test
  void getRbacRolesMultipleIdsAndCombinations() throws IOException {
    // first, start by just testing how it handles multiple workspace IDs.

    final UUID workspaceId1 = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();
    final UUID workspaceId3 = UUID.randomUUID();

    when(mHeaderResolver.resolveWorkspace(any())).thenReturn(List.of(workspaceId1, workspaceId2, workspaceId3));
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId1, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_ADMIN);
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId2, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_EDITOR);
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId3, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_READER);

    Collection<String> actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));

    // minimum common permission is reader, so expect only reader auth role set.
    Assertions.assertEquals(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_READER), actualRoles);

    // change reader to admin, now expect editor as minimum role set.
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId3, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_ADMIN);

    actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    Assertions.assertEquals(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_EDITOR), actualRoles);

    // now add organization IDs into the mix.

    final UUID organizationId1 = UUID.randomUUID();
    final UUID organizationId2 = UUID.randomUUID();

    when(mHeaderResolver.resolveOrganization(any())).thenReturn(List.of(organizationId1, organizationId2));
    when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(organizationId1, AUTH_USER_ID))
        .thenReturn(PermissionType.ORGANIZATION_EDITOR);
    when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(organizationId2, AUTH_USER_ID))
        .thenReturn(PermissionType.ORGANIZATION_MEMBER);

    actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    final Set<String> expectedRoles = new HashSet<>();
    // still expect editor as minimum workspace role set
    expectedRoles.addAll(WorkspaceAuthRole.buildWorkspaceAuthRolesSet(WorkspaceAuthRole.WORKSPACE_EDITOR));
    // expect minimum org role set to be member
    expectedRoles.addAll(OrganizationAuthRole.buildOrganizationAuthRolesSet(OrganizationAuthRole.ORGANIZATION_MEMBER));
    Assertions.assertEquals(expectedRoles, actualRoles);

    // now add target user auth id into the mix, at first NOT matching AUTH_USER_ID.
    // no new role should be added.

    when(mHeaderResolver.resolveAuthUserIds(any())).thenReturn(Set.of(UUID.randomUUID().toString()));

    actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    Assertions.assertEquals(expectedRoles, actualRoles);

    // now make target user auth id match AUTH_USER_ID.
    // expect SELF role to be added.

    when(mHeaderResolver.resolveAuthUserIds(any())).thenReturn(Set.of(AUTH_USER_ID));

    actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    expectedRoles.add(AuthRoleConstants.SELF);
    Assertions.assertEquals(expectedRoles, actualRoles);
  }

  @Test
  void getRbacRolesMultipleIdsAndCombinationsWithNull() throws IOException {
    final UUID workspaceId1 = UUID.randomUUID();
    final UUID workspaceId2 = UUID.randomUUID();
    final UUID workspaceId3 = UUID.randomUUID();

    when(mHeaderResolver.resolveWorkspace(any())).thenReturn(List.of(workspaceId1, workspaceId2, workspaceId3));
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId1, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_ADMIN);
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId2, AUTH_USER_ID))
        .thenReturn(PermissionType.WORKSPACE_EDITOR);
    when(mPermissionPersistence.findPermissionTypeForUserAndWorkspace(workspaceId3, AUTH_USER_ID))
        .thenReturn(null); // should cause overall role to be NONE

    Set<String> actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));

    // one workspace of the three has no permission, so expect NONE
    Assertions.assertEquals(Set.of(WorkspaceAuthRole.NONE.getLabel()), actualRoles);

    // now test the same for organizations
    final UUID organizationId1 = UUID.randomUUID();
    final UUID organizationId2 = UUID.randomUUID();

    when(mHeaderResolver.resolveOrganization(any())).thenReturn(List.of(organizationId1, organizationId2));
    when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(organizationId1, AUTH_USER_ID))
        .thenReturn(PermissionType.ORGANIZATION_EDITOR);
    when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(organizationId2, AUTH_USER_ID))
        .thenReturn(null);

    actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    Assertions.assertEquals(Set.of(OrganizationAuthRole.NONE.getLabel()), actualRoles);
  }

  @Test
  void getInstanceAdminRoles() {
    final Set<String> expectedRoles = Set.of(
        AuthRole.ADMIN.getLabel(),
        AuthRole.EDITOR.getLabel(),
        AuthRole.READER.getLabel(),
        AuthRole.AUTHENTICATED_USER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_ADMIN.getLabel(),
        WorkspaceAuthRole.WORKSPACE_EDITOR.getLabel(),
        WorkspaceAuthRole.WORKSPACE_RUNNER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_READER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_ADMIN.getLabel(),
        OrganizationAuthRole.ORGANIZATION_EDITOR.getLabel(),
        OrganizationAuthRole.ORGANIZATION_RUNNER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_READER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_MEMBER.getLabel());

    final Set<String> actualRoles = RbacRoleHelper.getInstanceAdminRoles();

    Assertions.assertEquals(expectedRoles, actualRoles);
  }

  @Test
  void getRbacRolesFromOrganizationLevel() throws IOException {
    // You're an organization admin ONLY, and we require workspace admin -> pass (because org_admin will
    // grant workspace_admin role)
    final UUID organizationId = UUID.randomUUID();

    when(mHeaderResolver.resolveOrganization(any())).thenReturn(List.of(organizationId));
    when(mPermissionPersistence.findPermissionTypeForUserAndOrganization(organizationId, AUTH_USER_ID))
        .thenReturn(PermissionType.ORGANIZATION_ADMIN);

    final Set<String> actualRoles = new HashSet<>(rbacRoleHelper.getRbacRoles(AUTH_USER_ID, mRequest));
    final Set<String> expectedRoles = Set.of(
        WorkspaceAuthRole.WORKSPACE_ADMIN.getLabel(),
        WorkspaceAuthRole.WORKSPACE_EDITOR.getLabel(),
        WorkspaceAuthRole.WORKSPACE_READER.getLabel(),
        WorkspaceAuthRole.WORKSPACE_RUNNER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_ADMIN.getLabel(),
        OrganizationAuthRole.ORGANIZATION_EDITOR.getLabel(),
        OrganizationAuthRole.ORGANIZATION_READER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_RUNNER.getLabel(),
        OrganizationAuthRole.ORGANIZATION_MEMBER.getLabel());
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
