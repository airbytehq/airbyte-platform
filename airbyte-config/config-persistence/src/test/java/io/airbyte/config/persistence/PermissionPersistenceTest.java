/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.UserPermission;
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
    final ConfigRepository configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database),
        new WorkspaceServiceJooqImpl(database));
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
    UserPermission userPermission = userPermissions.get(0);
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

  @Nested
  class UpdatePermission {

    final Permission instanceAdminPermission = MockData.permission1;
    final Permission workspaceOwnerPermission = MockData.permission2;
    final Permission organizationAdminPermission = MockData.permission5;
    final Permission organizationReaderPermission = MockData.permission7;

    @Test
    void shouldNotUpdateInstanceAdminPermissionTypeToOthers() throws IOException {
      final Permission update = new Permission()
          .withPermissionId(instanceAdminPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR) // another permission type
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

    @Test
    void shouldNotUpdateWorkspaceLevelPermissionTypeToOrganizationLevelPermissions() throws IOException {
      final Permission update = new Permission()
          .withPermissionId(workspaceOwnerPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_EDITOR) // org level permission type
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

    @Test
    void shouldNotUpdateOrganizationLevelPermissionTypeToWorkspaceLevelPermissions() throws IOException {
      final Permission update = new Permission()
          .withPermissionId(organizationReaderPermission.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_ADMIN) // workspace level permission type
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored
      Assertions.assertThrows(DataAccessException.class, () -> permissionPersistence.writePermission(update));
    }

    @Test
    void updateExistingWorkspaceLevelPermissionCannotChangeUserWorkspaceId() throws IOException {

      final Permission update = new Permission()
          .withPermissionId(workspaceOwnerPermission.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_EDITOR) // another workspace level permission type, changing from "owner" to "editor".
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored

      final Permission expectedPermission = new Permission()
          .withPermissionId(workspaceOwnerPermission.getPermissionId())
          .withPermissionType(PermissionType.WORKSPACE_EDITOR) // only type should change
          .withUserId(workspaceOwnerPermission.getUserId())
          .withWorkspaceId(workspaceOwnerPermission.getWorkspaceId())
          .withOrganizationId(workspaceOwnerPermission.getOrganizationId());

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(update));
      Assertions.assertEquals(expectedPermission, permissionPersistence.getPermission(update.getPermissionId()).orElseThrow());
    }

    @Test
    void updateExistingOrganizationLevelPermissionCannotChangeUserOrganizationId() throws IOException {

      final Permission update = new Permission()
          .withPermissionId(organizationReaderPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_MEMBER) // another org level permission type, changing from "admin" to "member".
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored

      final Permission expectedPermission = new Permission()
          .withPermissionId(organizationReaderPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_MEMBER) // only type should change
          .withUserId(organizationReaderPermission.getUserId())
          .withWorkspaceId(organizationReaderPermission.getWorkspaceId())
          .withOrganizationId(organizationReaderPermission.getOrganizationId());

      Assertions.assertDoesNotThrow(() -> permissionPersistence.writePermission(update));
      Assertions.assertEquals(expectedPermission, permissionPersistence.getPermission(update.getPermissionId()).orElseThrow());
    }

    @Test
    void shouldNotRemoveTheLastOrganizationAdminFromAnOrganization() throws IOException {

      final Permission update = new Permission()
          .withPermissionId(organizationAdminPermission.getPermissionId())
          .withPermissionType(PermissionType.ORGANIZATION_MEMBER) // demote the last org admin role into a member role
          .withUserId(UUID.randomUUID()) // should be ignored
          .withWorkspaceId(UUID.randomUUID()) // should be ignored
          .withOrganizationId(UUID.randomUUID()); // should be ignored

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

      final DataAccessException thrown = Assertions.assertThrows(DataAccessException.class,
          () -> permissionPersistence.writePermission(orgAdmin2.withPermissionType(PermissionType.ORGANIZATION_EDITOR)));

      Assertions.assertTrue(thrown.getCause() instanceof SQLOperationNotAllowedException);

      // make sure the last org-admin is still an org-admin, ie the update did not persist
      Assertions.assertEquals(
          PermissionType.ORGANIZATION_ADMIN,
          permissionPersistence.getPermission(orgAdmin2.getPermissionId()).orElseThrow().getPermissionType());
    }

  }

}
