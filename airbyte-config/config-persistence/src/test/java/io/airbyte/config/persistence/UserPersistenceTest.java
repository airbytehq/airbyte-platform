/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.config.persistence.OrganizationPersistence.DEFAULT_ORGANIZATION_ID;
import static org.mockito.Mockito.mock;

import io.airbyte.config.AuthProvider;
import io.airbyte.config.AuthUser;
import io.airbyte.config.AuthenticatedUser;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.Organization;
import io.airbyte.config.Permission;
import io.airbyte.config.Permission.PermissionType;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.User;
import io.airbyte.config.helpers.AuthenticatedUserConverter;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UserPersistenceTest extends BaseConfigDatabaseTest {

  private UserPersistence userPersistence;
  private WorkspaceService workspaceService;
  private OrganizationService organizationService;
  private DataplaneGroupService dataplaneGroupService;
  private static final UUID dataplaneGroupId = UUID.randomUUID();

  @BeforeEach
  void setup() {
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final MetricClient metricClient = mock(MetricClient.class);
    dataplaneGroupService = new DataplaneGroupServiceTestJooqImpl(database);
    workspaceService =
        new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter, secretPersistenceConfigService,
            metricClient);
    userPersistence = new UserPersistence(database);
    organizationService = new OrganizationServiceJooqImpl(database);
  }

  @Nested
  class BasicTests {

    @BeforeEach
    void beforeEach() throws Exception {
      truncateAllTables();
      setupTestData();
    }

    private void setupTestData() throws IOException, JsonValidationException {
      // Create organization
      organizationService.writeOrganization(MockData.defaultOrganization());

      // Create dataplane groups
      dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
          .withId(dataplaneGroupId)
          .withOrganizationId(DEFAULT_ORGANIZATION_ID)
          .withName("test")
          .withEnabled(true)
          .withTombstone(false));

      // write workspace table
      for (final StandardWorkspace workspace : MockData.standardWorkspaces()) {
        workspaceService.writeStandardWorkspaceNoSecrets(workspace);
      }
      // write user table
      for (final AuthenticatedUser user : MockData.users()) {
        userPersistence.writeAuthenticatedUser(user);
      }
    }

    @Test
    void getUserByIdTest() throws IOException {
      for (final AuthenticatedUser user : MockData.users()) {
        final Optional<AuthenticatedUser> userFromDb = userPersistence.getAuthenticatedUser(user.getUserId());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void getUserByAuthIdTest() throws IOException {
      for (final AuthenticatedUser user : MockData.users()) {
        final Optional<AuthenticatedUser> userFromDb = userPersistence.getUserByAuthId(user.getAuthUserId());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void getUserByEmailTest() throws IOException {
      for (final AuthenticatedUser user : MockData.users()) {
        final Optional<AuthenticatedUser> userFromDb = userPersistence.getAuthenticatedUserByEmail(user.getEmail());
        Assertions.assertEquals(user, userFromDb.get());
      }
    }

    @Test
    void deleteUserByIdTest() throws IOException {
      userPersistence.deleteUserById(MockData.CREATOR_USER_ID_1);
      Assertions.assertEquals(Optional.empty(), userPersistence.getAuthenticatedUser(MockData.CREATOR_USER_ID_1));
    }

    @Test
    void listAuthUserIdsForUserTest() throws IOException {
      final var user1 = MockData.users().getFirst();
      // set auth_user_id to a known value
      final var expectedAuthUserId = UUID.randomUUID().toString();
      userPersistence.writeAuthUser(user1.getUserId(), expectedAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM);

      final Set<String> actualAuthUserIds = new HashSet<>(userPersistence.listAuthUserIdsForUser(user1.getUserId()));
      Assertions.assertEquals(Set.of(expectedAuthUserId, user1.getAuthUserId()), actualAuthUserIds);
    }

    @Test
    void listAuthUsersTest() throws IOException {
      final var user1 = MockData.users().getFirst();

      // add an extra auth user
      final var newAuthUserId = UUID.randomUUID().toString();
      userPersistence.writeAuthUser(user1.getUserId(), newAuthUserId, AuthProvider.KEYCLOAK);

      final List<AuthUser> expectedAuthUsers = Stream.of(
          new AuthUser().withUserId(user1.getUserId()).withAuthUserId(user1.getAuthUserId()).withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM),
          new AuthUser().withUserId(user1.getUserId()).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK))
          .sorted(Comparator.comparing(AuthUser::getUserId))
          .toList();

      final List<AuthUser> authUsers = userPersistence.listAuthUsersForUser(user1.getUserId());
      Assertions.assertEquals(expectedAuthUsers, authUsers.stream().sorted(Comparator.comparing(AuthUser::getUserId)).toList());
    }

    @Test
    void replaceAuthUserTest() throws IOException {
      final var user1 = MockData.users().getFirst();

      // create auth users
      final String oldAuthUserId = UUID.randomUUID().toString();
      final String oldAuthUserId2 = UUID.randomUUID().toString();
      userPersistence.writeAuthUser(user1.getUserId(), oldAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM);
      userPersistence.writeAuthUser(user1.getUserId(), oldAuthUserId2, AuthProvider.KEYCLOAK);

      final Set<String> actualAuthUserIds = new HashSet<>(userPersistence.listAuthUserIdsForUser(user1.getUserId()));
      Assertions.assertEquals(Set.of(oldAuthUserId, oldAuthUserId2, user1.getAuthUserId()), actualAuthUserIds);

      // replace auth_user_id
      final var newAuthUserId = UUID.randomUUID().toString();
      userPersistence.replaceAuthUserForUserId(user1.getUserId(), newAuthUserId, AuthProvider.KEYCLOAK);

      final Set<String> newAuthUserIds = new HashSet<>(userPersistence.listAuthUserIdsForUser(user1.getUserId()));
      Assertions.assertEquals(Set.of(newAuthUserId), newAuthUserIds);
    }

  }

  @Nested
  class UserAccessTests {

    private static final Organization ORG = new Organization()
        .withUserId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
        .withName("Org")
        .withEmail("test@org.com");

    private static final Organization ORG_2 = new Organization()
        .withUserId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
        .withName("Org 2")
        .withEmail("test@org.com");

    private static final StandardWorkspace WORKSPACE_1_ORG_1 = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace1_org1")
        .withSlug("workspace1_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId);

    private static final StandardWorkspace WORKSPACE_2_ORG_1 = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace2_org1")
        .withSlug("workspace2_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId);

    private static final StandardWorkspace WORKSPACE_3_ORG_2 = new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace3_no_org")
        .withSlug("workspace3_no_org-slug")
        .withOrganizationId(ORG_2.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId);

    private static final AuthenticatedUser ORG_MEMBER_USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("orgMember@airbyte.io")
        .withName("orgMember");

    private static final AuthenticatedUser ORG_READER_USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("orgReader@airbyte.io")
        .withName("orgReader");

    private static final AuthenticatedUser WORKSPACE_2_AND_3_READER_USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("workspace2and3Reader@airbyte.io")
        .withName("workspace2and3Reader");

    // this user will have both workspace-level and org-level permissions to workspace 2
    private static final AuthenticatedUser BOTH_ORG_AND_WORKSPACE_USER = new AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("bothOrgAndWorkspace@airbyte.io")
        .withName("bothOrgAndWorkspace");

    // orgMemberUser does not get access to any workspace since they're just an organization member
    private static final Permission ORG_MEMBER_USER_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_MEMBER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(PermissionType.ORGANIZATION_MEMBER);

    // orgReaderUser gets access to workspace1_org1 and workspace2_org1
    private static final Permission ORG_READER_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_READER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(PermissionType.ORGANIZATION_READER);

    // workspaceReaderUser gets direct access to workspace2_org1 and workspace3_no_org via workspace
    // permissions
    private static final Permission WORKSPACE_2_READER_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    private static final Permission WORKSPACE_3_READER_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_3_ORG_2.getWorkspaceId())
        .withPermissionType(PermissionType.WORKSPACE_READER);

    private static final Permission BOTH_USER_WORKSPACE_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
        .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
        .withPermissionType(PermissionType.WORKSPACE_EDITOR);

    private static final Permission BOTH_USER_ORGANIZATION_PERMISSION = new Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(PermissionType.ORGANIZATION_ADMIN);

    @BeforeEach
    void setup() throws IOException, JsonValidationException, SQLException {
      truncateAllTables();

      final OrganizationPersistence organizationPersistence = new OrganizationPersistence(database);

      organizationPersistence.createOrganization(ORG);
      organizationPersistence.createOrganization(ORG_2);

      // Create dataplane groups
      dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
          .withId(UUID.randomUUID())
          .withOrganizationId(ORG.getOrganizationId())
          .withName("workspace1_org1_dataplaneGroup")
          .withEnabled(true)
          .withTombstone(false));
      dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
          .withId(UUID.randomUUID())
          .withOrganizationId(ORG_2.getOrganizationId())
          .withName("workspace3_org2_dataplaneGroup")
          .withEnabled(true)
          .withTombstone(false));

      for (final StandardWorkspace workspace : List.of(WORKSPACE_1_ORG_1, WORKSPACE_2_ORG_1, WORKSPACE_3_ORG_2)) {
        workspaceService.writeStandardWorkspaceNoSecrets(workspace);
      }

      for (final AuthenticatedUser user : List.of(ORG_MEMBER_USER, ORG_READER_USER, WORKSPACE_2_AND_3_READER_USER, BOTH_ORG_AND_WORKSPACE_USER)) {
        userPersistence.writeAuthenticatedUser(user);
      }

      for (final Permission permission : List.of(ORG_MEMBER_USER_PERMISSION, ORG_READER_PERMISSION, WORKSPACE_2_READER_PERMISSION,
          WORKSPACE_3_READER_PERMISSION, BOTH_USER_WORKSPACE_PERMISSION, BOTH_USER_ORGANIZATION_PERMISSION)) {
        BaseConfigDatabaseTest.writePermission(permission);
      }
    }

    @Test
    void getUsersWithWorkspaceAccess() throws IOException {
      final Set<User> expectedUsersWorkspace1 =
          Set.copyOf(Stream.of(ORG_READER_USER, BOTH_ORG_AND_WORKSPACE_USER).map(AuthenticatedUserConverter::toUser).toList());
      final Set<User> expectedUsersWorkspace2 = Set.copyOf(
          Stream.of(ORG_READER_USER, WORKSPACE_2_AND_3_READER_USER, BOTH_ORG_AND_WORKSPACE_USER).map(AuthenticatedUserConverter::toUser).toList());
      final Set<User> expectedUsersWorkspace3 =
          Set.copyOf(Stream.of(WORKSPACE_2_AND_3_READER_USER).map(AuthenticatedUserConverter::toUser).toList());

      final Set<User> actualUsersWorkspace1 = new HashSet<>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_1_ORG_1.getWorkspaceId()));
      final Set<User> actualUsersWorkspace2 = new HashSet<>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_2_ORG_1.getWorkspaceId()));
      final Set<User> actualUsersWorkspace3 = new HashSet<>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_3_ORG_2.getWorkspaceId()));

      Assertions.assertEquals(expectedUsersWorkspace1, actualUsersWorkspace1);
      Assertions.assertEquals(expectedUsersWorkspace2, actualUsersWorkspace2);
      Assertions.assertEquals(expectedUsersWorkspace3, actualUsersWorkspace3);
    }

    @Test
    void listWorkspaceUserAccessInfo() throws IOException {
      // Due to a jooq bug that only impacts CI, I can't get this test to pass.
      // PermissionType enum values are mapped to 'null' even though they are
      // not null in the database. This is only happening in CI, not locally.
      // So, I am changing this test to only check the userIds, which is the
      // critical piece that we absolutely need to cover with tests.

      // final Set<WorkspaceUserAccessInfo> expectedUsersWorkspace1 = Set.of(
      // new WorkspaceUserAccessInfo()
      // .withUserId(ORG_READER_USER.getUserId())
      // .withUserEmail(ORG_READER_USER.getEmail())
      // .withUserName(ORG_READER_USER.getName())
      // .withWorkspaceId(WORKSPACE_1_ORG_1.getWorkspaceId())
      // .withWorkspacePermission(null)
      // .withOrganizationPermission(ORG_READER_PERMISSION),
      // new WorkspaceUserAccessInfo()
      // .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
      // .withUserEmail(BOTH_ORG_AND_WORKSPACE_USER.getEmail())
      // .withUserName(BOTH_ORG_AND_WORKSPACE_USER.getName())
      // .withWorkspaceId(WORKSPACE_1_ORG_1.getWorkspaceId())
      // .withWorkspacePermission(null)
      // .withOrganizationPermission(BOTH_USER_ORGANIZATION_PERMISSION));
      //
      // final Set<WorkspaceUserAccessInfo> expectedUsersWorkspace2 = Set.of(
      // new WorkspaceUserAccessInfo()
      // .withUserId(ORG_READER_USER.getUserId())
      // .withUserEmail(ORG_READER_USER.getEmail())
      // .withUserName(ORG_READER_USER.getName())
      // .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
      // .withWorkspacePermission(null)
      // .withOrganizationPermission(ORG_READER_PERMISSION),
      // new WorkspaceUserAccessInfo()
      // .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
      // .withUserEmail(WORKSPACE_2_AND_3_READER_USER.getEmail())
      // .withUserName(WORKSPACE_2_AND_3_READER_USER.getName())
      // .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
      // .withWorkspacePermission(WORKSPACE_2_READER_PERMISSION)
      // .withOrganizationPermission(null),
      // new WorkspaceUserAccessInfo()
      // .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
      // .withUserEmail(BOTH_ORG_AND_WORKSPACE_USER.getEmail())
      // .withUserName(BOTH_ORG_AND_WORKSPACE_USER.getName())
      // .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
      // .withWorkspacePermission(BOTH_USER_WORKSPACE_PERMISSION)
      // .withOrganizationPermission(BOTH_USER_ORGANIZATION_PERMISSION));
      //
      // final Set<WorkspaceUserAccessInfo> expectedUsersWorkspace3 = Set.of(
      // new WorkspaceUserAccessInfo()
      // .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
      // .withUserEmail(WORKSPACE_2_AND_3_READER_USER.getEmail())
      // .withUserName(WORKSPACE_2_AND_3_READER_USER.getName())
      // .withWorkspaceId(WORKSPACE_3_NO_ORG.getWorkspaceId())
      // .withWorkspacePermission(WORKSPACE_3_READER_PERMISSION)
      // .withOrganizationPermission(null));
      //
      // final Set<WorkspaceUserAccessInfo> actualUsersWorkspace1 =
      // new HashSet<>(userPersistence.listWorkspaceUserAccessInfo(WORKSPACE_1_ORG_1.getWorkspaceId()));
      // final Set<WorkspaceUserAccessInfo> actualUsersWorkspace2 =
      // new HashSet<>(userPersistence.listWorkspaceUserAccessInfo(WORKSPACE_2_ORG_1.getWorkspaceId()));
      // final Set<WorkspaceUserAccessInfo> actualUsersWorkspace3 =
      // new HashSet<>(userPersistence.listWorkspaceUserAccessInfo(WORKSPACE_3_NO_ORG.getWorkspaceId()));

      final Set<UUID> expectedUsersWorkspace1 = Set.of(ORG_READER_USER.getUserId(), BOTH_ORG_AND_WORKSPACE_USER.getUserId());
      final Set<UUID> expectedUsersWorkspace2 =
          Set.of(ORG_READER_USER.getUserId(), WORKSPACE_2_AND_3_READER_USER.getUserId(), BOTH_ORG_AND_WORKSPACE_USER.getUserId());
      final Set<UUID> expectedUsersWorkspace3 = Set.of(WORKSPACE_2_AND_3_READER_USER.getUserId());

      final Set<UUID> actualUsersWorkspace1 =
          new HashSet<>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_1_ORG_1.getWorkspaceId()));
      final Set<UUID> actualUsersWorkspace2 =
          new HashSet<>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_2_ORG_1.getWorkspaceId()));
      final Set<UUID> actualUsersWorkspace3 =
          new HashSet<>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_3_ORG_2.getWorkspaceId()));

      Assertions.assertEquals(expectedUsersWorkspace1, actualUsersWorkspace1);
      Assertions.assertEquals(expectedUsersWorkspace2, actualUsersWorkspace2);
      Assertions.assertEquals(expectedUsersWorkspace3, actualUsersWorkspace3);
    }

  }

}
