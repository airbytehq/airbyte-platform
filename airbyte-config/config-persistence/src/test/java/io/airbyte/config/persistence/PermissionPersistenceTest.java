/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.UserPermission;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PermissionPersistenceTest extends BaseConfigDatabaseTest {

  private PermissionPersistence permissionPersistence;
  private OrganizationPersistence organizationPersistence;

  @BeforeEach
  void beforeEach() throws Exception {
    permissionPersistence = new PermissionPersistence(database);
    organizationPersistence = new OrganizationPersistence(database);
    truncateAllTables();
    setupTestData();
  }

  private void setupTestData() throws Exception {
    final UserPersistence userPersistence = new UserPersistence(database);
    final DataplaneGroupService dataplaneGroupService = mock(DataplaneGroupService.class);
    when(dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(any(), any()))
        .thenReturn(new DataplaneGroup().withId(UUID.randomUUID()));

    organizationPersistence.createOrganization(MockData.defaultOrganization());
    final WorkspaceService workspaceService = new WorkspaceServiceJooqImpl(
        database,
        mock(TestClient.class),
        mock(SecretsRepositoryReader.class),
        mock(SecretsRepositoryWriter.class),
        mock(SecretPersistenceConfigService.class),
        mock(MetricClient.class));
    // write workspace table
    for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
      workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    }
    // write user table
    for (final AuthenticatedUser user : MockData.users()) {
      userPersistence.writeAuthenticatedUser(user);
    }

    for (final Organization organization : MockData.organizations()) {
      organizationPersistence.createOrganization(organization);
    }

    // write permission table
    for (final Permission permission : MockData.permissions()) {
      BaseConfigDatabaseTest.writePermission(permission);
    }
  }

  @Test
  void listUsersInOrganizationTest() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listUsersInOrganization(MockData.ORGANIZATION_ID_1);
    Assertions.assertEquals(1, userPermissions.size());
  }

  @Test
  void listInstanceUsersTest() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listInstanceAdminUsers();
    Assertions.assertEquals(1, userPermissions.size());
    final UserPermission userPermission = userPermissions.get(0);
    Assertions.assertEquals(MockData.CREATOR_USER_ID_1, userPermission.getUser().getUserId());
  }

  @Test
  void findUsersInWorkspaceTest() throws Exception {
    final PermissionType permissionType = permissionPersistence
        .findPermissionTypeForUserAndWorkspace(MockData.WORKSPACE_ID_2, MockData.CREATOR_USER_ID_5.toString());
    Assertions.assertEquals(PermissionType.WORKSPACE_ADMIN, permissionType);
  }

  @Test
  void findUsersInOrganizationTest() throws Exception {
    final PermissionType permissionType = permissionPersistence
        .findPermissionTypeForUserAndOrganization(MockData.ORGANIZATION_ID_2, MockData.CREATOR_USER_ID_5.toString());
    Assertions.assertEquals(PermissionType.ORGANIZATION_READER, permissionType);
  }

  @Test
  void listPermissionsForOrganizationTest() throws Exception {
    final List<UserPermission> actualPermissions = permissionPersistence.listPermissionsForOrganization(MockData.ORGANIZATION_ID_1);
    final List<Permission> expectedPermissions = MockData.permissions().stream()
        .filter(p -> p.getOrganizationId() != null && p.getOrganizationId().equals(MockData.ORGANIZATION_ID_1))
        .toList();

    Assertions.assertEquals(expectedPermissions.size(), actualPermissions.size());
    for (final UserPermission actualPermission : actualPermissions) {
      Assertions.assertTrue(expectedPermissions.stream()
          .anyMatch(expectedPermission -> expectedPermission.getPermissionId().equals(actualPermission.getPermission().getPermissionId())
              && actualPermission.getUser().getUserId().equals(expectedPermission.getUserId())));
    }
  }

}
