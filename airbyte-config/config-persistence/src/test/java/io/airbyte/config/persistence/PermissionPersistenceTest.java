/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    final ConfigRepository configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));

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
  void deletePermissionByIdTest() throws IOException {
    permissionPersistence.deletePermissionById(MockData.PERMISSION_ID_4);
    Assertions.assertEquals(Optional.empty(), permissionPersistence.getPermission(MockData.PERMISSION_ID_4));
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

  @Nested
  class WritePermission {

    @Test
    void createNewPermission() throws IOException {
      final Permission permission = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_1)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_1);

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(permission));
      Assertions.assertEquals(permission, permissionPersistence.getPermission(permission.getPermissionId()).orElseThrow());
    }

    @Test
    void createPermissionExceptionTest() {
      // writing permissions against Permission table constraint should throw db exception.

      // invalid permission 1: permission type cannot be null
      final Permission invalidPermission1 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(UUID.randomUUID())
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(null);

      // invalid permission 2: for workspace level permission, org id should be null and workspace id
      // cannot be null
      final Permission invalidPermission2 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(UUID.randomUUID())
          .withOrganizationId(UUID.randomUUID())
          .withPermissionType(PermissionType.WORKSPACE_OWNER);

      // invalid permission 3: for organization level permission, org id cannot be null and workspace id
      // should be null
      final Permission invalidPermission3 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(UUID.randomUUID())
          .withWorkspaceId(UUID.randomUUID())
          .withPermissionType(PermissionType.ORGANIZATION_MEMBER);

      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(invalidPermission1));
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(invalidPermission2));
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(invalidPermission3));
    }

  }

  /**
   * Note that while the Persistence layer allows updates to ID fields, the API layer does not. Since
   * blocking such updates is an explicit API-level concern, our persistence layer tests cover updates
   * to ID fields.
   */
  @Nested
  class UpdatePermission {

    final Permission instanceAdminPermission = MockData.permission1;
    final Permission workspaceAdminPermission = MockData.permission4;
    final Permission organizationReaderPermission = MockData.permission7;

    @Test
    void updateWorkspacePermission() throws IOException {
      final Permission update = workspaceAdminPermission
          .withPermissionType(PermissionType.WORKSPACE_READER) // change to a different workspace-level permission type
          .withWorkspaceId(MockData.WORKSPACE_ID_2) // change to a different workspace ID
          .withUserId(MockData.CREATOR_USER_ID_1); // change to a different user ID

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(update));
      final Permission updated = permissionPersistence.getPermission(update.getPermissionId()).orElseThrow();

      Assertions.assertEquals(update, updated);
    }

    @Test
    void updateOrganizationPermission() throws IOException {
      final Permission update = organizationReaderPermission
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR) // change to a different organization-level permission type
          .withOrganizationId(MockData.ORGANIZATION_ID_3) // change to a different organization ID
          .withUserId(MockData.CREATOR_USER_ID_1); // change to a different user ID

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(update));
      final Permission updated = permissionPersistence.getPermission(update.getPermissionId()).orElseThrow();

      Assertions.assertEquals(update, updated);
    }

    @Test
    void updateInstanceAdminPermission() throws IOException {
      final Permission update = instanceAdminPermission
          .withUserId(MockData.CREATOR_USER_ID_2); // change to a different user ID

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(update));
      final Permission updated = permissionPersistence.getPermission(update.getPermissionId()).orElseThrow();

      Assertions.assertEquals(instanceAdminPermission.getPermissionId(), updated.getPermissionId());
      Assertions.assertEquals(PermissionType.INSTANCE_ADMIN, updated.getPermissionType());
      Assertions.assertEquals(MockData.CREATOR_USER_ID_2, updated.getUserId());
    }

    @Test
    void shouldNotUpdateInstanceAdminPermissionTypeToOthers() {
      final Permission update = new Permission()
          .withPermissionId(instanceAdminPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR); // another permission type
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

    @Test
    void shouldNotUpdateWorkspaceLevelPermissionTypeToOrganizationLevelPermissions() {
      final Permission update = new Permission()
          .withPermissionId(workspaceAdminPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR); // org level permission type
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

    @Test
    void shouldNotUpdateOrganizationLevelPermissionTypeToWorkspaceLevelPermissions() {
      final Permission update = new Permission()
          .withPermissionId(organizationReaderPermission.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_ADMIN); // workspace level permission type
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

  }

  @Nested
  class SpecializedCases {

    @Test
    void cannotDeleteLastOrganizationAdmin() throws IOException {
      final Permission orgAdmin1 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_1);
      final Permission orgAdmin2 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_2);

      permissionPersistence.writePermission(orgAdmin1);
      permissionPersistence.writePermission(orgAdmin2);

      Assertions.assertDoesNotThrow(() -> permissionPersistence.deletePermissionById(orgAdmin1.getPermissionId()));
      final DataAccessException thrown =
          Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.deletePermissionById(orgAdmin2.getPermissionId()));

      Assertions.assertTrue(thrown.getCause() instanceof SQLOperationNotAllowedException);

      // make sure the last org-admin permission is still present in the DB
      Assertions.assertEquals(orgAdmin2, permissionPersistence.getPermission(orgAdmin2.getPermissionId()).orElseThrow());
    }

    @Test
    void cannotDemoteLastOrganizationAdmin() throws IOException {
      final Permission orgAdmin1 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_1);
      final Permission orgAdmin2 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_2);

      permissionPersistence.writePermission(orgAdmin1);
      permissionPersistence.writePermission(orgAdmin2);

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(orgAdmin1.withPermissionType(PermissionType.ORGANIZATION_EDITOR)));

      final Permission demotionUpdate = orgAdmin2
          .withPermissionId(orgAdmin2.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR);

      final DataAccessException thrown = Assertions.assertThrows(DataAccessException.class,
          () -> permissionPersistence.writePermission(demotionUpdate));

      Assertions.assertTrue(thrown.getCause() instanceof SQLOperationNotAllowedException);

      // make sure the last org-admin is still an org-admin, ie the update did not persist
      Assertions.assertEquals(
          PermissionType.ORGANIZATION_ADMIN,
          permissionPersistence.getPermission(orgAdmin2.getPermissionId()).orElseThrow().getPermissionType());
    }

    @Test
    void cannotChangeLastOrganizationAdminToADifferentOrg() throws IOException {
      final Permission orgAdmin1 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_1);
      final Permission orgAdmin2 = new Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(MockData.ORGANIZATION_ID_2)
          .withPermissionType(PermissionType.ORGANIZATION_ADMIN)
          .withUserId(MockData.CREATOR_USER_ID_2);

      permissionPersistence.writePermission(orgAdmin1);
      permissionPersistence.writePermission(orgAdmin2);

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(orgAdmin1.withPermissionType(PermissionType.ORGANIZATION_EDITOR)));

      final Permission demotionUpdate = orgAdmin2
          .withPermissionId(orgAdmin2.getPermissionId())
          .withOrganizationId(MockData.ORGANIZATION_ID_3);

      final DataAccessException thrown = Assertions.assertThrows(DataAccessException.class,
          () -> permissionPersistence.writePermission(demotionUpdate));

      Assertions.assertTrue(thrown.getCause() instanceof SQLOperationNotAllowedException);

      // make sure the last org-admin is still in the original org, ie the update did not persist
      Assertions.assertEquals(
          MockData.ORGANIZATION_ID_2,
          permissionPersistence.getPermission(orgAdmin2.getPermissionId()).orElseThrow().getOrganizationId());
    }

  }

}
