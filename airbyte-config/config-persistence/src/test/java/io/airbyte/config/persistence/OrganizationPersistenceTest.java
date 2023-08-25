/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.*;

import io.airbyte.config.Organization;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OrganizationPersistenceTest extends BaseConfigDatabaseTest {

  OrganizationPersistence organizationPersistence;

  @BeforeEach
  void beforeEach() throws Exception {
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

}
