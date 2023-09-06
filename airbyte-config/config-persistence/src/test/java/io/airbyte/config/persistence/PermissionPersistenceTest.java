/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.UserPermission;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
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

  private void setupTestData() throws IOException, JsonValidationException {
    final UserPersistence userPersistence = new UserPersistence(database);
    final ConfigRepository configRepository = new ConfigRepository(database, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER);
    // write workspace table
    for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
      configRepository.writeStandardWorkspaceNoSecrets(workspace);
    }
    // write user table
    for (final User user : MockData.users()) {
      userPersistence.writeUser(user);
    }

    for (final Organization organization : MockData.organizations()) {
      organizationPersistence.createOrganization(organization);
    }

    // write permission table
    for (final Permission permission : MockData.permissions()) {
      permissionPersistence.writePermission(permission);
    }
  }

  @Test
  void getPermissionByIdTest() throws IOException {
    for (final Permission permission : MockData.permissions()) {
      final Optional<Permission> permissionFromDb = permissionPersistence.getPermission(permission.getPermissionId());
      Assertions.assertEquals(permission, permissionFromDb.get());
    }
  }

  @Test
  void listPermissionByUserTest() throws IOException {
    final List<Permission> permissions = permissionPersistence.listPermissionsByUser(MockData.CREATOR_USER_ID_1);
    Assertions.assertEquals(2, permissions.size());
  }

  @Test
  void listPermissionByWorkspaceTest() throws IOException {
    final List<Permission> permissions = permissionPersistence.listPermissionByWorkspace(MockData.WORKSPACE_ID_1);
    Assertions.assertEquals(2, permissions.size());
  }

  @Test
  void deletePermissionByIdTest() throws IOException {
    permissionPersistence.deletePermissionById(MockData.PERMISSION_ID_4);
    Assertions.assertEquals(Optional.empty(), permissionPersistence.getPermission(MockData.PERMISSION_ID_4));
  }

  @Test
  void deletePermissionByUserIdTest() throws IOException {
    permissionPersistence.deletePermissionByUserId(MockData.CREATOR_USER_ID_1);
    Assertions.assertEquals(0, permissionPersistence.listPermissionsByUser(MockData.CREATOR_USER_ID_1).size());
  }

  @Test
  void deletePermissionByWorkspaceIdTest() throws IOException {
    permissionPersistence.deletePermissionByWorkspaceId(MockData.WORKSPACE_ID_2);
    Assertions.assertEquals(0, permissionPersistence.listPermissionByWorkspace(MockData.WORKSPACE_ID_2).size());
  }

  @Test
  void listUsersInOrganizationTest() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listUsersInOrganization(MockData.ORGANIZATION_ID_1);
    Assertions.assertEquals(1, userPermissions.size());
  }

  @Test
  void listUsersInWorkspaceTest() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listUsersInWorkspace(MockData.WORKSPACE_ID_1);
    Assertions.assertEquals(2, userPermissions.size());
  }

  @Test
  void listInstanceUsersTest() throws IOException {
    final List<UserPermission> userPermissions = permissionPersistence.listInstanceAdminUsers();
    Assertions.assertEquals(1, userPermissions.size());
    UserPermission userPermission = userPermissions.get(0);
    Assertions.assertEquals(MockData.CREATOR_USER_ID_1, userPermission.getUser().getUserId());
  }

  @Test
  void findUsersInWorkspaceTest() throws Exception {
    final PermissionType permissionType = permissionPersistence
        .findPermissionTypeForUserAndWorkspace(MockData.WORKSPACE_ID_2, MockData.CREATOR_USER_ID_5.toString(), AuthProvider.KEYCLOAK);
    Assertions.assertEquals(PermissionType.WORKSPACE_ADMIN, permissionType);
  }

  @Test
  void findUsersInOrganizationTest() throws Exception {
    final PermissionType permissionType = permissionPersistence
        .findPermissionTypeForUserAndOrganization(MockData.ORGANIZATION_ID_2, MockData.CREATOR_USER_ID_5.toString(), AuthProvider.KEYCLOAK);
    Assertions.assertEquals(PermissionType.ORGANIZATION_READER, permissionType);
  }

}
