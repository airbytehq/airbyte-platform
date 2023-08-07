/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.Permission;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PermissionPersistenceTest extends BaseConfigDatabaseTest {

  private PermissionPersistence permissionPersistence;

  @BeforeEach
  void beforeEach() throws Exception {
    permissionPersistence = new PermissionPersistence(database);
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

}
