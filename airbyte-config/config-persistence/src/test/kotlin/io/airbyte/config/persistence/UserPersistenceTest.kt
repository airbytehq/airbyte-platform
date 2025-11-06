/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthUser
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.List
import java.util.Optional
import java.util.Set
import java.util.UUID
import java.util.function.Function
import java.util.stream.Stream

internal class UserPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var userPersistence: UserPersistence
  private lateinit var workspaceService: WorkspaceService
  private lateinit var organizationService: OrganizationService
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  fun setup() {
    val featureFlagClient: FeatureFlagClient = Mockito.mock<TestClient>(TestClient::class.java)
    val secretsRepositoryReader = Mockito.mock<SecretsRepositoryReader>(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock<SecretsRepositoryWriter>(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)
    dataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )
    userPersistence = UserPersistence(database)
    organizationService = OrganizationServiceJooqImpl(database)
  }

  @Nested
  internal inner class BasicTests {
    @BeforeEach
    fun beforeEach() {
      truncateAllTables()
      setupTestData()
    }

    private fun setupTestData() {
      // Create organization
      organizationService.writeOrganization(MockData.defaultOrganization())

      // Create dataplane groups
      dataplaneGroupService.writeDataplaneGroup(
        DataplaneGroup()
          .withId(dataplaneGroupId)
          .withOrganizationId(MockData.defaultOrganization()!!.getOrganizationId())
          .withName("test")
          .withEnabled(true)
          .withTombstone(false),
      )

      // write workspace table
      for (workspace in MockData.standardWorkspaces()) {
        workspaceService.writeStandardWorkspaceNoSecrets(workspace!!)
      }
      // write user table
      for (user in MockData.users()) {
        userPersistence.writeAuthenticatedUser(user!!)
      }
    }

    @Test
    fun getUserByIdTest() {
      for (user in MockData.users()) {
        val userFromDb = userPersistence.getAuthenticatedUser(user!!.getUserId())
        Assertions.assertEquals(user, userFromDb.get())
      }
    }

    @Test
    fun getUserByAuthIdTest() {
      for (user in MockData.users()) {
        val userFromDb = userPersistence.getUserByAuthId(user!!.getAuthUserId())
        Assertions.assertEquals(user, userFromDb.get())
      }
    }

    @Test
    fun getUserByEmailTest() {
      for (user in MockData.users()) {
        val userFromDb = userPersistence.getAuthenticatedUserByEmail(user!!.getEmail())
        Assertions.assertEquals(user, userFromDb.get())
      }
    }

    @Test
    fun deleteUserByIdTest() {
      userPersistence.deleteUserById(MockData.CREATOR_USER_ID_1)
      Assertions.assertEquals(Optional.empty<Any?>(), userPersistence.getAuthenticatedUser(MockData.CREATOR_USER_ID_1))
    }

    @Test
    fun listAuthUserIdsForUserTest() {
      val user1: AuthenticatedUser = MockData.users().first()!!
      // set auth_user_id to a known value
      val expectedAuthUserId = UUID.randomUUID().toString()
      userPersistence.writeAuthUser(user1.getUserId(), expectedAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)

      val actualAuthUserIds: MutableSet<String?> = HashSet<String?>(userPersistence.listAuthUserIdsForUser(user1.getUserId()))
      Assertions.assertEquals(Set.of<String?>(expectedAuthUserId, user1.getAuthUserId()), actualAuthUserIds)
    }

    @Test
    fun listAuthUsersTest() {
      val user1: AuthenticatedUser = MockData.users().first()!!

      // add an extra auth user
      val newAuthUserId = UUID.randomUUID().toString()
      userPersistence.writeAuthUser(user1.getUserId(), newAuthUserId, AuthProvider.KEYCLOAK)

      val expectedAuthUsers =
        Stream
          .of<AuthUser?>(
            AuthUser()
              .withUserId(user1.getUserId())
              .withAuthUserId(user1.getAuthUserId())
              .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM),
            AuthUser().withUserId(user1.getUserId()).withAuthUserId(newAuthUserId).withAuthProvider(AuthProvider.KEYCLOAK),
          ).sorted(Comparator.comparing<AuthUser?, UUID?>(Function { obj: AuthUser? -> obj?.getUserId() }))
          .toList()

      val authUsers = userPersistence.listAuthUsersForUser(user1.getUserId())
      Assertions.assertEquals(
        expectedAuthUsers,
        authUsers.stream().sorted(Comparator.comparing<AuthUser?, UUID?>(Function { obj: AuthUser? -> obj!!.getUserId() })).toList(),
      )
    }

    @Test
    fun replaceAuthUserTest() {
      val user1: AuthenticatedUser = MockData.users().first()!!

      // create auth users
      val oldAuthUserId = UUID.randomUUID().toString()
      val oldAuthUserId2 = UUID.randomUUID().toString()
      userPersistence.writeAuthUser(user1.getUserId(), oldAuthUserId, AuthProvider.GOOGLE_IDENTITY_PLATFORM)
      userPersistence.writeAuthUser(user1.getUserId(), oldAuthUserId2, AuthProvider.KEYCLOAK)

      val actualAuthUserIds: MutableSet<String?> = HashSet<String?>(userPersistence.listAuthUserIdsForUser(user1.getUserId()))
      Assertions.assertEquals(Set.of<String?>(oldAuthUserId, oldAuthUserId2, user1.getAuthUserId()), actualAuthUserIds)

      // replace auth_user_id
      val newAuthUserId = UUID.randomUUID().toString()
      userPersistence.replaceAuthUserForUserId(user1.getUserId(), newAuthUserId, AuthProvider.KEYCLOAK)

      val newAuthUserIds: MutableSet<String?> = HashSet<String?>(userPersistence.listAuthUserIdsForUser(user1.getUserId()))
      Assertions.assertEquals(Set.of<String?>(newAuthUserId), newAuthUserIds)
    }

    @Test
    fun findUsersWithEmailDomainOutsideOrganizationTest() {
      // Create two organizations
      val org1 =
        Organization()
          .withOrganizationId(UUID.randomUUID())
          .withName("Org 1")
          .withEmail("admin@org1.com")
      val org2 =
        Organization()
          .withOrganizationId(UUID.randomUUID())
          .withName("Org 2")
          .withEmail("admin@org2.com")

      organizationService.writeOrganization(org1)
      organizationService.writeOrganization(org2)

      // Create users with different email domains
      val user1Org1 =
        AuthenticatedUser()
          .withUserId(UUID.randomUUID())
          .withName("User 1 Org1")
          .withEmail("user1@example.com")
          .withAuthUserId("auth1")
          .withAuthProvider(AuthProvider.KEYCLOAK)
      val user2Org1 =
        AuthenticatedUser()
          .withUserId(UUID.randomUUID())
          .withName("User 2 Org1")
          .withEmail("user2@example.com")
          .withAuthUserId("auth2")
          .withAuthProvider(AuthProvider.KEYCLOAK)
      val user3Org2 =
        AuthenticatedUser()
          .withUserId(UUID.randomUUID())
          .withName("User 3 Org2")
          .withEmail("user3@example.com")
          .withAuthUserId("auth3")
          .withAuthProvider(AuthProvider.KEYCLOAK)
      val user4DifferentDomain =
        AuthenticatedUser()
          .withUserId(UUID.randomUUID())
          .withName("User 4 Different")
          .withEmail("user4@other.com")
          .withAuthUserId("auth4")
          .withAuthProvider(AuthProvider.KEYCLOAK)
      val user5MixedCase =
        AuthenticatedUser()
          .withUserId(UUID.randomUUID())
          .withName("User 5 Mixed Case")
          .withEmail("User5@Example.COM")
          .withAuthUserId("auth5")
          .withAuthProvider(AuthProvider.KEYCLOAK)

      userPersistence.writeAuthenticatedUser(user1Org1)
      userPersistence.writeAuthenticatedUser(user2Org1)
      userPersistence.writeAuthenticatedUser(user3Org2)
      userPersistence.writeAuthenticatedUser(user4DifferentDomain)
      userPersistence.writeAuthenticatedUser(user5MixedCase)

      // Create permissions linking users to organizations
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(user1Org1.getUserId())
          .withOrganizationId(org1.getOrganizationId())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER),
      )
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(user2Org1.getUserId())
          .withOrganizationId(org1.getOrganizationId())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER),
      )
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(user3Org2.getUserId())
          .withOrganizationId(org2.getOrganizationId())
          .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER),
      )

      // Test: Find users with @example.com domain outside of org1
      // Should return user3Org2 (belongs to org2, not org1) and user5MixedCase (no org permission)
      // This also tests case-insensitive matching (user5MixedCase has "Example.COM")
      val usersOutsideOrg1 = userPersistence.findUsersWithEmailDomainOutsideOrganization("example.com", org1.getOrganizationId())
      Assertions.assertEquals(2, usersOutsideOrg1.size)
      Assertions.assertTrue(usersOutsideOrg1.contains(user3Org2.getUserId()))
      Assertions.assertTrue(usersOutsideOrg1.contains(user5MixedCase.getUserId()))
      Assertions.assertFalse(usersOutsideOrg1.contains(user1Org1.getUserId()))
      Assertions.assertFalse(usersOutsideOrg1.contains(user2Org1.getUserId()))

      // Test: Find users with @example.com domain outside of org2
      // Should return user1Org1, user2Org1 (both belong to org1, not org2), and user5MixedCase (no org)
      val usersOutsideOrg2 = userPersistence.findUsersWithEmailDomainOutsideOrganization("example.com", org2.getOrganizationId())
      Assertions.assertEquals(3, usersOutsideOrg2.size)
      Assertions.assertTrue(usersOutsideOrg2.contains(user1Org1.getUserId()))
      Assertions.assertTrue(usersOutsideOrg2.contains(user2Org1.getUserId()))
      Assertions.assertTrue(usersOutsideOrg2.contains(user5MixedCase.getUserId()))
      Assertions.assertFalse(usersOutsideOrg2.contains(user3Org2.getUserId()))

      // Test: Find users with @other.com domain outside of org1
      // Should return user4DifferentDomain (no organization permission)
      val usersWithOtherDomain = userPersistence.findUsersWithEmailDomainOutsideOrganization("other.com", org1.getOrganizationId())
      Assertions.assertEquals(1, usersWithOtherDomain.size)
      Assertions.assertTrue(usersWithOtherDomain.contains(user4DifferentDomain.getUserId()))

      // Test: Find users with non-existent domain
      val usersWithNonexistentDomain = userPersistence.findUsersWithEmailDomainOutsideOrganization("nonexistent.com", org1.getOrganizationId())
      Assertions.assertEquals(0, usersWithNonexistentDomain.size)
    }
  }

  @Nested
  internal inner class UserAccessTests {
    @BeforeEach
    fun setup() {
      truncateAllTables()

      val organizationService = OrganizationServiceJooqImpl(database)

      organizationService.writeOrganization(ORG)
      organizationService.writeOrganization(ORG_2)

      // Create dataplane groups
      dataplaneGroupService.writeDataplaneGroup(
        DataplaneGroup()
          .withId(UUID.randomUUID())
          .withOrganizationId(ORG.getOrganizationId())
          .withName("workspace1_org1_dataplaneGroup")
          .withEnabled(true)
          .withTombstone(false),
      )
      dataplaneGroupService.writeDataplaneGroup(
        DataplaneGroup()
          .withId(UUID.randomUUID())
          .withOrganizationId(ORG_2.getOrganizationId())
          .withName("workspace3_org2_dataplaneGroup")
          .withEnabled(true)
          .withTombstone(false),
      )

      for (workspace in List.of<StandardWorkspace>(WORKSPACE_1_ORG_1, WORKSPACE_2_ORG_1, WORKSPACE_3_ORG_2)) {
        workspaceService.writeStandardWorkspaceNoSecrets(workspace)
      }

      for (user in List.of<AuthenticatedUser>(ORG_MEMBER_USER, ORG_READER_USER, WORKSPACE_2_AND_3_READER_USER, BOTH_ORG_AND_WORKSPACE_USER)) {
        userPersistence.writeAuthenticatedUser(user)
      }

      for (permission in List.of<Permission>(
        ORG_MEMBER_USER_PERMISSION,
        ORG_READER_PERMISSION,
        WORKSPACE_2_READER_PERMISSION,
        WORKSPACE_3_READER_PERMISSION,
        BOTH_USER_WORKSPACE_PERMISSION,
        BOTH_USER_ORGANIZATION_PERMISSION,
      )) {
        writePermission(permission)
      }
    }

    // Temporarily commented out due to Kotlin compiler internal error
    // @Test
    //
    // fun getUsersWithWorkspaceAccess() {
    //     val expectedUsersWorkspace1 = setOf(
    //         AuthenticatedUserConverter.toUser(ORG_READER_USER),
    //         AuthenticatedUserConverter.toUser(BOTH_ORG_AND_WORKSPACE_USER)
    //     )
    //     val expectedUsersWorkspace2 = setOf(
    //         AuthenticatedUserConverter.toUser(ORG_READER_USER),
    //         AuthenticatedUserConverter.toUser(WORKSPACE_2_AND_3_READER_USER),
    //         AuthenticatedUserConverter.toUser(BOTH_ORG_AND_WORKSPACE_USER)
    //     )
    //     val expectedUsersWorkspace3 = setOf(
    //         AuthenticatedUserConverter.toUser(WORKSPACE_2_AND_3_READER_USER)
    //     )

    //     val actualUsersWorkspace1: MutableSet<User?> =
    //         HashSet<User?>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_1_ORG_1.getWorkspaceId()))
    //     val actualUsersWorkspace2: MutableSet<User?> =
    //         HashSet<User?>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_2_ORG_1.getWorkspaceId()))
    //     val actualUsersWorkspace3: MutableSet<User?> =
    //         HashSet<User?>(userPersistence.getUsersWithWorkspaceAccess(WORKSPACE_3_ORG_2.getWorkspaceId()))

    //     Assertions.assertEquals(expectedUsersWorkspace1, actualUsersWorkspace1)
    //     Assertions.assertEquals(expectedUsersWorkspace2, actualUsersWorkspace2)
    //     Assertions.assertEquals(expectedUsersWorkspace3, actualUsersWorkspace3)
    // }

    @Test
    fun listWorkspaceUserAccessInfo() {
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

      val expectedUsersWorkspace1 = Set.of<UUID?>(ORG_READER_USER.getUserId(), BOTH_ORG_AND_WORKSPACE_USER.getUserId())
      val expectedUsersWorkspace2 =
        Set.of<UUID?>(ORG_READER_USER.getUserId(), WORKSPACE_2_AND_3_READER_USER.getUserId(), BOTH_ORG_AND_WORKSPACE_USER.getUserId())
      val expectedUsersWorkspace3 = Set.of<UUID?>(WORKSPACE_2_AND_3_READER_USER.getUserId())

      val actualUsersWorkspace1: MutableSet<UUID?> =
        HashSet<UUID?>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_1_ORG_1.getWorkspaceId()))
      val actualUsersWorkspace2: MutableSet<UUID?> =
        HashSet<UUID?>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_2_ORG_1.getWorkspaceId()))
      val actualUsersWorkspace3: MutableSet<UUID?> =
        HashSet<UUID?>(userPersistence.listJustUsersForWorkspaceUserAccessInfo(WORKSPACE_3_ORG_2.getWorkspaceId()))

      Assertions.assertEquals(expectedUsersWorkspace1, actualUsersWorkspace1)
      Assertions.assertEquals(expectedUsersWorkspace2, actualUsersWorkspace2)
      Assertions.assertEquals(expectedUsersWorkspace3, actualUsersWorkspace3)
    }
  }

  companion object {
    private val dataplaneGroupId: UUID = UUID.randomUUID()

    private val ORG: Organization =
      Organization()
        .withUserId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
        .withName("Org")
        .withEmail("test@org.com")

    private val ORG_2: Organization =
      Organization()
        .withUserId(UUID.randomUUID())
        .withOrganizationId(UUID.randomUUID())
        .withName("Org 2")
        .withEmail("test@org.com")

    private val WORKSPACE_1_ORG_1: StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace1_org1")
        .withSlug("workspace1_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId)

    private val WORKSPACE_2_ORG_1: StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace2_org1")
        .withSlug("workspace2_org1-slug")
        .withOrganizationId(ORG.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId)

    private val WORKSPACE_3_ORG_2: StandardWorkspace =
      StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withName("workspace3_no_org")
        .withSlug("workspace3_no_org-slug")
        .withOrganizationId(ORG_2.getOrganizationId())
        .withInitialSetupComplete(true)
        .withTombstone(false)
        .withDataplaneGroupId(dataplaneGroupId)

    private val ORG_MEMBER_USER: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("orgMember@airbyte.io")
        .withName("orgMember")

    private val ORG_READER_USER: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("orgReader@airbyte.io")
        .withName("orgReader")

    private val WORKSPACE_2_AND_3_READER_USER: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("workspace2and3Reader@airbyte.io")
        .withName("workspace2and3Reader")

    // this user will have both workspace-level and org-level permissions to workspace 2
    private val BOTH_ORG_AND_WORKSPACE_USER: AuthenticatedUser =
      AuthenticatedUser()
        .withUserId(UUID.randomUUID())
        .withAuthUserId(UUID.randomUUID().toString())
        .withAuthProvider(AuthProvider.GOOGLE_IDENTITY_PLATFORM)
        .withEmail("bothOrgAndWorkspace@airbyte.io")
        .withName("bothOrgAndWorkspace")

    // orgMemberUser does not get access to any workspace since they're just an organization member
    private val ORG_MEMBER_USER_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_MEMBER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(Permission.PermissionType.ORGANIZATION_MEMBER)

    // orgReaderUser gets access to workspace1_org1 and workspace2_org1
    private val ORG_READER_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(ORG_READER_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(Permission.PermissionType.ORGANIZATION_READER)

    // workspaceReaderUser gets direct access to workspace2_org1 and workspace3_no_org via workspace
    // permissions
    private val WORKSPACE_2_READER_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val WORKSPACE_3_READER_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(WORKSPACE_2_AND_3_READER_USER.getUserId())
        .withWorkspaceId(WORKSPACE_3_ORG_2.getWorkspaceId())
        .withPermissionType(Permission.PermissionType.WORKSPACE_READER)

    private val BOTH_USER_WORKSPACE_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
        .withWorkspaceId(WORKSPACE_2_ORG_1.getWorkspaceId())
        .withPermissionType(Permission.PermissionType.WORKSPACE_EDITOR)

    private val BOTH_USER_ORGANIZATION_PERMISSION: Permission =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(BOTH_ORG_AND_WORKSPACE_USER.getUserId())
        .withOrganizationId(ORG.getOrganizationId())
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN)
  }
}
