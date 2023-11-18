/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.mockito.Mockito.mock;

import io.airbyte.config.Geography;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.User.AuthProvider;
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
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UserPersistenceTest extends BaseConfigDatabaseTest {

  private UserPersistence userPersistence;
  private ConfigRepository configRepository;

  @BeforeEach
  void setup() {
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
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
    userPersistence = new UserPersistence(database);
  }

  @Nested
  class BasicTests {

    @BeforeEach
    void beforeEach() throws Exception {
      truncateAllTables();
      setupTestData();
    }

    private void setupTestData() throws IOException, JsonValidationException {
      // write workspace table
      for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
        configRepository.writeStandardWorkspaceNoSecrets(workspace);
      }
      // write user table
      for (final User user : MockData.users()) {
        userPersistence.writeUser(user);
      }
    }

    @Test
    void getUserByIdTest() throws IOException {
      for (final User user : MockData.users()) {
        final Optional<User> userFromDb = userPersistence.getUser(user.getUserId());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void getUserByAuthIdTest() throws IOException {
      for (final User user : MockData.users()) {
        final Optional<User> userFromDb = userPersistence.getUserByAuthId(user.getAuthUserId());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void getUserByEmailTest() throws IOException {
      for (final User user : MockData.users()) {
        final Optional<User> userFromDb = userPersistence.getUserByEmail(user.getEmail());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void deleteUserByIdTest() throws IOException {
      userPersistence.deleteUserById(MockData.CREATOR_USER_ID_1);
      Assertions.assertEquals(Optional.empty(), userPersistence.getUser(MockData.CREATOR_USER_ID_1));
    }

  }

  @Nested
  class GetUsersWithWorkspaceAccess {

    private static final Organization ORG = new Organization()
        .withUserId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
        .withName("Org")
        .withEmail("test@org.com")
        .withPba(false)
        .withOrgLevelBilling(false);

    private static final StandardWorkspace WORKSPACE_1_ORG_1 = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace1_org1")
        .withSlug("workspace1_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);

    private static final StandardWorkspace WORKSPACE_2_ORG_1 = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace2_org1")
        .withSlug("workspace2_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);

    private static final StandardWorkspace WORKSPACE_3_NO_ORG = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace3_no_org")
        .withSlug("workspace3_no_org-slug")
        .withOrganizationId(null)
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDefaultGeography(Geography.AUTO);

    private static final User ORG_MEMBER_USER = new User()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("test1@airbyte.io")
        .withName("test1");

    private static final User ORG_READER_USER = new User()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("test2@airbyte.io")
        .withName("test2");

    private static final User WORKSPACE_READER_USER = new User()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("test3@airbyte.io")
        .withName("test3");

    // orgMemberUser does not get access to any workspace since they're just an organization member
    private static final Permission PERMISSION_1 = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_MEMBER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(PermissionType.ORGANIZATION_MEMBER);

    // orgReaderUser gets access to workspace1_org1 and workspace2_org1
    private static final Permission PERMISSION_2 = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_READER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(PermissionType.ORGANIZATION_READER);

    // workspaceReaderUser gets direct access to workspace2_org1 and workspace3_no_org via workspace
    // permissions
    private static final Permission PERMISSION_3 = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    private static final Permission PERMISSION_4 = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_3_NO_ORG.getWorkspaceId())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    @BeforeEach
    void setup() throws IOException, JsonValidationException, SQLException {
      truncateAllTables();

      final PermissionPersistence permissionPersistence = new PermissionPersistence(database);
      final OrganizationPersistence organizationPersistence = new OrganizationPersistence(database);

      organizationPersistence.createOrganization(ORG);

      for (final StandardWorkspace workspace : List.of(WORKSPACE_1_ORG_1, WORKSPACE_2_ORG_1, WORKSPACE_3_NO_ORG)) {
        configRepository.writeStandardWorkspaceNoSecrets(workspace);
      }

      for (final User user : List.of(ORG_MEMBER_USER, ORG_READER_USER, WORKSPACE_READER_USER)) {
        userPersistence.writeUser(user);
      }

      for (final Permission permission : List.of(PERMISSION_1, PERMISSION_2, PERMISSION_3, PERMISSION_4)) {
        permissionPersistence.writePermission(permission);
      }
    }

    @Test
    void getUsersWithWorkspaceAccess() throws IOException {
      final List<User> expectedUsersWorkspace1 = List.of(ORG_READER_USER);
      final List<User> expectedUsersWorkspace2 = List.of(ORG_READER_USER, WORKSPACE_READER_USER);
      final List<User> expectedUsersWorkspace3 = List.of(WORKSPACE_READER_USER);

      final List<User> actualUsersWorkspace1 = userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_1_ORG_1.getWorkspaceId());
      final List<User> actualUsersWorkspace2 = userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_2_ORG_1.getWorkspaceId());
      final List<User> actualUsersWorkspace3 = userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_3_NO_ORG.getWorkspaceId());

      Assertions.assertEquals(expectedUsersWorkspace1, actualUsersWorkspace1);
      Assertions.assertEquals(expectedUsersWorkspace2, actualUsersWorkspace2);
      Assertions.assertEquals(expectedUsersWorkspace3, actualUsersWorkspace3);
    }

  }

}
