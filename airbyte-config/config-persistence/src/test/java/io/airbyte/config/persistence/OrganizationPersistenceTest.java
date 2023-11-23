/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.SsoConfig;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.TestClient;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
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
  private WorkspaceService workspaceService;
  private TestClient featureFlagClient;
  private SecretsRepositoryReader secretsRepositoryReader;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private SecretPersistenceConfigService secretPersistenceConfigService;

  @BeforeEach
  void beforeEach() throws Exception {
    permissionPersistence = new PermissionPersistence(database);
    userPersistence = new UserPersistence(database);
    organizationPersistence = new OrganizationPersistence(database);
    featureFlagClient = new TestClient();
    secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    workspaceService = new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService);
    truncateAllTables();

    for (final Organization organization : MockData.organizations()) {
      organizationPersistence.createOrganization(organization);
    }
    for (final SsoConfig ssoConfig : MockData.ssoConfigs()) {
      organizationPersistence.createSsoConfig(ssoConfig);
    }
  }

  @Test
  void createOrganization() throws Exception {
    Organization organization = new Organization()
        .withOrganizationId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withEmail("octavia@airbyte.io")
        .withName("new org")
        .withPba(false)
        .withOrgLevelBilling(false);
    organizationPersistence.createOrganization(organization);
    Optional<Organization> result = organizationPersistence.getOrganization(organization.getOrganizationId());
    assertTrue(result.isPresent());
    assertEquals(organization, result.get());
  }

  @Test
  void createSsoConfig() throws Exception {
    final Organization org = new Organization()
        .withOrganizationId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withEmail("test@test.com")
        .withName("new org")
        .withPba(false)
        .withOrgLevelBilling(false);
    final SsoConfig ssoConfig = new SsoConfig()
        .withSsoConfigId(UUID.randomUUID())
        .withOrganizationId(org.getOrganizationId())
        .withKeycloakRealm("realm");
    organizationPersistence.createOrganization(org);
    organizationPersistence.createSsoConfig(ssoConfig);
    Optional<SsoConfig> result = organizationPersistence.getSsoConfigForOrganization(org.getOrganizationId());
    assertTrue(result.isPresent());
    assertEquals(ssoConfig, result.get());
  }

  @Test
  void getOrganization() throws Exception {
    Optional<Organization> result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1);
    assertTrue(result.isPresent());
    // expecting organization 1 to have sso realm from sso config 1
    final Organization expected = MockData.organizations().get(0).withSsoRealm(MockData.ssoConfigs().get(0).getKeycloakRealm());
    assertEquals(expected, result.get());
  }

  @Test
  void getOrganization_notExist() throws Exception {
    Optional<Organization> result = organizationPersistence.getOrganization(UUID.randomUUID());
    assertFalse(result.isPresent());
  }

  @Test
  void getOrganizationByWorkspaceId() throws IOException, JsonValidationException {
    // write a workspace that belongs to org 1
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    workspace.setOrganizationId(MockData.ORGANIZATION_ID_1);
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);

    final Optional<Organization> result = organizationPersistence.getOrganizationByWorkspaceId(MockData.WORKSPACE_ID_1);
    assertTrue(result.isPresent());
    assertEquals(MockData.ORGANIZATION_ID_1, result.get().getOrganizationId());
  }

  @Test
  void getOrganizationByWorkspaceId_notInAnOrg() throws IOException, JsonValidationException {
    // write a workspace that does not belong to an org
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    workspaceService.writeStandardWorkspaceNoSecrets(workspace);

    final Optional<Organization> result = organizationPersistence.getOrganizationByWorkspaceId(MockData.WORKSPACE_ID_1);
    assertTrue(result.isEmpty());
  }

  @Test
  void getSsoConfigForOrganization() throws Exception {
    Optional<SsoConfig> result = organizationPersistence.getSsoConfigForOrganization(MockData.ORGANIZATION_ID_1);
    assertTrue(result.isPresent());
    assertEquals(MockData.SSO_CONFIG_ID_1, result.get().getSsoConfigId());
  }

  @Test
  void getSsoConfigByRealmName() throws Exception {
    final SsoConfig ssoConfig = MockData.ssoConfigs().get(0);
    final Optional<SsoConfig> result = organizationPersistence.getSsoConfigByRealmName(ssoConfig.getKeycloakRealm());
    assertTrue(result.isPresent());
    assertEquals(ssoConfig, result.get());
  }

  @Test
  void updateOrganization() throws Exception {
    final Organization updatedOrganization = MockData.organizations().get(0);

    updatedOrganization.setName("new name");
    updatedOrganization.setEmail("newemail@airbyte.io");
    updatedOrganization.setPba(!updatedOrganization.getPba());
    updatedOrganization.setOrgLevelBilling(!updatedOrganization.getOrgLevelBilling());
    updatedOrganization.setUserId(MockData.CREATOR_USER_ID_5);

    organizationPersistence.updateOrganization(updatedOrganization);

    final Organization result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1).orElseThrow();

    assertEquals(updatedOrganization.getOrganizationId(), result.getOrganizationId());
    assertEquals(updatedOrganization.getName(), result.getName());
    assertEquals(updatedOrganization.getEmail(), result.getEmail());
    assertEquals(updatedOrganization.getPba(), result.getPba());
    assertEquals(updatedOrganization.getOrgLevelBilling(), result.getOrgLevelBilling());
    assertEquals(updatedOrganization.getUserId(), result.getUserId());
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
        .withEmail("email1")
        .withPba(false)
        .withOrgLevelBilling(false));
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
        .withEmail("email2")
        .withPba(false)
        .withOrgLevelBilling(false));
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
        .withEmail("email3")
        .withPba(false)
        .withOrgLevelBilling(false));
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
