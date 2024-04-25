/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.mockito.Mockito.mock;

import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.UserPermission;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
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

  private void setupTestData() throws Exception {
    final UserPersistence userPersistence = new UserPersistence(database);
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    final ConnectionService connectionService = mock(ConnectionService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    final ConfigRepository configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        connectionService,
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService,
            connectionService,
            actorDefinitionVersionUpdater),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService,
            connectionService,
            actorDefinitionVersionUpdater),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));

    organizationPersistence.createOrganization(MockData.defaultOrganization());

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
      BaseConfigDatabaseTest.writePermission(permission);
    }
  }

  @Test
  void permissionTypeTest() throws IOException {
    for (final Permission permission : MockData.permissions()) {
      final Optional<Permission> permissionFromDb = permissionPersistence.getPermission(permission.getPermissionId());
      if (permission.getPermissionType() == PermissionType.WORKSPACE_OWNER) {
        Assertions.assertEquals(PermissionType.WORKSPACE_ADMIN, permissionFromDb.get().getPermissionType());
      } else {
        Assertions.assertEquals(permission.getPermissionType(), permissionFromDb.get().getPermissionType());
      }
    }
  }

  @Test
  void getPermissionByIdTest() throws IOException {
    for (final Permission permission : MockData.permissions()) {
      final Optional<Permission> permissionFromDb = permissionPersistence.getPermission(permission.getPermissionId());
      Assertions.assertEquals(permission.getPermissionId(), permissionFromDb.get().getPermissionId());
      Assertions.assertEquals(permission.getOrganizationId(), permissionFromDb.get().getOrganizationId());
      Assertions.assertEquals(permission.getWorkspaceId(), permissionFromDb.get().getWorkspaceId());
      Assertions.assertEquals(permission.getUserId(), permissionFromDb.get().getUserId());
      // permission type "WORKSPACE_OWNER" will be converted into "WORKSPACE_ADMIN"
      Assertions.assertEquals(
          permission.getPermissionType() == PermissionType.WORKSPACE_OWNER ? PermissionType.WORKSPACE_ADMIN : permission.getPermissionType(),
          permissionFromDb.get().getPermissionType());
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

  @Test
  void isUserInstanceAdmin() throws IOException {
    final User user1 = MockData.users().get(0);
    Assertions.assertEquals(user1.getUserId(), MockData.permission1.getUserId());
    Assertions.assertEquals(MockData.permission1.getPermissionType(), PermissionType.INSTANCE_ADMIN);
    Assertions.assertTrue(permissionPersistence.isUserInstanceAdmin(user1.getUserId()));

    final User user2 = MockData.users().get(1);
    Assertions.assertEquals(user2.getUserId(), MockData.permission2.getUserId());
    Assertions.assertNotEquals(MockData.permission2.getPermissionType(), PermissionType.INSTANCE_ADMIN);
    Assertions.assertFalse(permissionPersistence.isUserInstanceAdmin(user2.getUserId()));
  }

  @Test
  void isAuthUserInstanceAdmin() throws IOException {
    final User user1 = MockData.users().get(0);
    Assertions.assertEquals(user1.getUserId(), MockData.permission1.getUserId());
    Assertions.assertEquals(MockData.permission1.getPermissionType(), PermissionType.INSTANCE_ADMIN);
    Assertions.assertTrue(permissionPersistence.isAuthUserInstanceAdmin(user1.getAuthUserId()));

    final User user2 = MockData.users().get(1);
    Assertions.assertEquals(user2.getUserId(), MockData.permission2.getUserId());
    Assertions.assertNotEquals(MockData.permission2.getPermissionType(), PermissionType.INSTANCE_ADMIN);
    Assertions.assertFalse(permissionPersistence.isAuthUserInstanceAdmin(user2.getAuthUserId()));
  }

}
