/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.ListOrganizationsByUserRequestBody;
import io.airbyte.api.model.generated.OrganizationCreateRequestBody;
import io.airbyte.api.model.generated.OrganizationIdRequestBody;
import io.airbyte.api.model.generated.OrganizationRead;
import io.airbyte.api.model.generated.OrganizationReadList;
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.config.Organization;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD")
class OrganizationsHandlerTest {

  private static final UUID ORGANIZATION_ID_1 = UUID.randomUUID();
  private static final String ORGANIZATION_NAME = "org_name";
  private static final String ORGANIZATION_EMAIL = "email@email.com";
  private static final String ORGANIZATION_SSO_REALM = "realm";
  private static final Organization ORGANIZATION =
      new Organization().withOrganizationId(ORGANIZATION_ID_1).withEmail(ORGANIZATION_EMAIL).withName(ORGANIZATION_NAME);
  private PermissionPersistence permissionPersistence;
  private OrganizationPersistence organizationPersistence;
  private Supplier<UUID> uuidSupplier;
  private OrganizationsHandler organizationsHandler;

  @BeforeEach
  void setup() {
    permissionPersistence = mock(PermissionPersistence.class);
    uuidSupplier = mock(Supplier.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    organizationsHandler = new OrganizationsHandler(organizationPersistence, permissionPersistence, uuidSupplier);
  }

  @Test
  void testCreateOrganization() throws Exception {

    final Organization newOrganization = new Organization().withOrganizationId(ORGANIZATION_ID_1).withEmail(ORGANIZATION_EMAIL)
        .withName(ORGANIZATION_NAME);
    when(uuidSupplier.get()).thenReturn(ORGANIZATION_ID_1);
    when(organizationPersistence.createOrganization(newOrganization)).thenReturn(newOrganization);
    doNothing().when(permissionPersistence).writePermission(any());

    final OrganizationRead result = organizationsHandler.createOrganization(
        new OrganizationCreateRequestBody().organizationName(ORGANIZATION_NAME).email(ORGANIZATION_EMAIL));
    assertEquals(ORGANIZATION_ID_1, result.getOrganizationId());
    assertEquals(ORGANIZATION_NAME, result.getOrganizationName());
    assertEquals(ORGANIZATION_EMAIL, result.getEmail());
  }

  @Test
  void testGetOrganization() throws Exception {
    when(organizationPersistence.getOrganization(ORGANIZATION_ID_1))
        .thenReturn(Optional.of(new Organization()
            .withOrganizationId(ORGANIZATION_ID_1)
            .withEmail(ORGANIZATION_EMAIL)
            .withName(ORGANIZATION_NAME)
            .withSsoRealm(ORGANIZATION_SSO_REALM)));

    final OrganizationRead result = organizationsHandler.getOrganization(new OrganizationIdRequestBody().organizationId(ORGANIZATION_ID_1));
    assertEquals(ORGANIZATION_ID_1, result.getOrganizationId());
    assertEquals(ORGANIZATION_NAME, result.getOrganizationName());
    assertEquals(ORGANIZATION_EMAIL, result.getEmail());
    assertEquals(ORGANIZATION_SSO_REALM, result.getSsoRealm());
  }

  @Test
  void testUpdateOrganization() throws Exception {
    // test updating org name and email
    final String newName = "new name";
    final String newEmail = "new email";
    when(organizationPersistence.getOrganization(ORGANIZATION_ID_1))
        .thenReturn(Optional.of(new Organization().withOrganizationId(ORGANIZATION_ID_1).withEmail(ORGANIZATION_EMAIL).withName(ORGANIZATION_NAME)));

    when(organizationPersistence.updateOrganization(ORGANIZATION
        .withName(newName)
        .withEmail(newEmail)))
            .thenReturn(ORGANIZATION.withName(newName).withEmail(newEmail));
    final OrganizationRead result =
        organizationsHandler.updateOrganization(new OrganizationUpdateRequestBody()
            .organizationId(ORGANIZATION_ID_1)
            .organizationName(newName)
            .email(newEmail));
    assertEquals(ORGANIZATION_ID_1, result.getOrganizationId());
    assertEquals(newName, result.getOrganizationName());
    assertEquals(newEmail, result.getEmail());
  }

  @Test
  void testListOrganizationsByUserWithoutPagination() throws Exception {
    final UUID userId = UUID.randomUUID();
    final UUID orgId = UUID.randomUUID();
    final ListOrganizationsByUserRequestBody request = new ListOrganizationsByUserRequestBody()
        .userId(userId)
        .nameContains("keyword");
    when(organizationPersistence.listOrganizationsByUserId(any(), any())).thenReturn(
        List.of(new Organization()
            .withOrganizationId(orgId)
            .withUserId(userId)
            .withName("org name")
            .withEmail(ORGANIZATION_EMAIL)
            .withSsoRealm(ORGANIZATION_SSO_REALM)));
    final OrganizationReadList expectedList = new OrganizationReadList()
        .organizations(List.of(new OrganizationRead()
            .organizationName("org name")
            .organizationId(orgId)
            .email(ORGANIZATION_EMAIL)
            .ssoRealm(ORGANIZATION_SSO_REALM)));
    assertEquals(expectedList, organizationsHandler.listOrganizationsByUser(request));

  }

  @Test
  void testListOrganizationsByUserWithPagination() throws Exception {
    final UUID userId = UUID.randomUUID();
    final UUID orgId = UUID.randomUUID();
    final ListOrganizationsByUserRequestBody request = new ListOrganizationsByUserRequestBody()
        .userId(userId)
        .nameContains("keyword")
        .pagination(new Pagination().pageSize(10).rowOffset(1));
    when(organizationPersistence.listOrganizationsByUserIdPaginated(any(), any())).thenReturn(
        List.of(new Organization()
            .withOrganizationId(orgId)
            .withUserId(userId)
            .withName(ORGANIZATION_NAME)
            .withEmail(ORGANIZATION_EMAIL)
            .withSsoRealm(ORGANIZATION_SSO_REALM)));
    final OrganizationReadList expectedList = new OrganizationReadList()
        .organizations(List.of(new OrganizationRead()
            .organizationName(ORGANIZATION_NAME)
            .organizationId(orgId)
            .email(ORGANIZATION_EMAIL)
            .ssoRealm(ORGANIZATION_SSO_REALM)));
    assertEquals(expectedList, organizationsHandler.listOrganizationsByUser(request));

  }

}
