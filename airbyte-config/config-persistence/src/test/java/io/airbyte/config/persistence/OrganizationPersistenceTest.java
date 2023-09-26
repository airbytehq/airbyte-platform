/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.*;

import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class OrganizationPersistenceTest extends BaseConfigDatabaseTest {

  private OrganizationPersistence organizationPersistence;
  private UserPersistence userPersistence;
  private PermissionPersistence permissionPersistence;

  @BeforeEach
  void beforeEach() throws Exception {
    permissionPersistence = new PermissionPersistence(database);
    userPersistence = new UserPersistence(database);
    organizationPersistence = new OrganizationPersistence(database);
    truncateAllTables();

    for (final Organization organization : MockData.organizations()) {
      organizationPersistence.createOrganization(organization);
    }
  }

  @Test
  void createOrganization() throws Exception {
    Organization organization = new Organization()
        .withOrganizationId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withEmail("octavia@airbyte.io")
        .withName("new org");
    organizationPersistence.createOrganization(organization);
    Optional<Organization> result = organizationPersistence.getOrganization(organization.getOrganizationId());
    assertTrue(result.isPresent());
    assertEquals(organization, result.get());
  }

  @Test
  void getOrganization() throws Exception {
    Optional<Organization> result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1);
    assertTrue(result.isPresent());
  }

  @Test
  void getOrganization_notExist() throws Exception {
    Optional<Organization> result = organizationPersistence.getOrganization(UUID.randomUUID());
    assertFalse(result.isPresent());
  }

  @Test
  void updateOrganization() throws Exception {
    String updatedName = "new name";
    Organization organizationUpdate = new Organization().withOrganizationId(MockData.ORGANIZATION_ID_1).withName(updatedName);
    organizationPersistence.updateOrganization(organizationUpdate);
    Optional<Organization> result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1);
    assertEquals(updatedName, result.get().getName());
  }

  @ParameterizedTest
  @CsvSource({
    "false, false",
    "true, false",
    "false, true",
    "true, true"
  })
  void testListOrganizationsByUserId(final Boolean withKeywordSearch, final Boolean withPagination) throws Exception {
    // create a user
    final UUID userId = UUID.randomUUID();
    userPersistence.writeUser(new User()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE));
    // create an org 1, name contains search "keyword"
    final UUID orgId1 = UUID.randomUUID();
    organizationPersistence.createOrganization(new Organization()
        .withOrganizationId(orgId1)
        .withUserId(userId)
        .withName("keyword")
        .withEmail("email1"));
    // grant user an admin access to org 1
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId1)
        .withUserId(userId)
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN));
    // create an org 2, name contains search "Keyword"
    final UUID orgId2 = UUID.randomUUID();
    organizationPersistence.createOrganization(new Organization()
        .withOrganizationId(orgId2)
        .withUserId(userId)
        .withName("Keyword")
        .withEmail("email2"));
    // grant user an editor access to org 2
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId2)
        .withUserId(userId)
        .withPermissionType(PermissionType.ORGANIZATION_EDITOR));
    // create an org 3, name contains search "randomName", owned by another user
    final UUID orgId3 = UUID.randomUUID();
    organizationPersistence.createOrganization(new Organization()
        .withOrganizationId(orgId3)
        .withUserId(UUID.randomUUID())
        .withName("randomName")
        .withEmail("email3"));
    // grant user a read access to org 3
    permissionPersistence.writePermission(new Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId3)
        .withUserId(userId)
        .withPermissionType(PermissionType.ORGANIZATION_READER));

    List<Organization> organizations;
    if (withKeywordSearch && withPagination) {
      organizations = organizationPersistence.listOrganizationsByUserIdPaginated(
          new ResourcesByUserQueryPaginated(userId, false, 10, 0), Optional.of("keyWord"));
      // Should not contain Org with "randomName"
      assertEquals(2, organizations.size());
    } else if (withPagination) {
      organizations = organizationPersistence.listOrganizationsByUserIdPaginated(
          new ResourcesByUserQueryPaginated(userId, false, 10, 0), Optional.empty());
      // Should also contain Org with "randomName"
      assertEquals(3, organizations.size());
      organizations = organizationPersistence.listOrganizationsByUserIdPaginated(
          new ResourcesByUserQueryPaginated(userId, false, 10, 2), Optional.empty());
      // Test pagination offset
      assertEquals(1, organizations.size());
    } else if (withKeywordSearch) { // no pagination in the request
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.of("keyWord"));
      assertEquals(2, organizations.size());
    } else { // no pagination no keyword search
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.empty());
      assertEquals(3, organizations.size());
    }
  }

}
