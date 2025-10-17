/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Organization
import io.airbyte.config.Permission
import io.airbyte.config.SsoConfig
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.Mockito
import java.io.IOException
import java.util.Optional
import java.util.UUID

internal class OrganizationPersistenceTest : BaseConfigDatabaseTest() {
  private lateinit var organizationPersistence: OrganizationPersistence
  private lateinit var userPersistence: UserPersistence
  private lateinit var workspaceService: WorkspaceService
  private lateinit var featureFlagClient: TestClient
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var secretsRepositoryWriter: SecretsRepositoryWriter
  private lateinit var secretPersistenceConfigService: SecretPersistenceConfigService
  private lateinit var dataplaneGroupService: DataplaneGroupService

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    userPersistence = UserPersistence(database)
    organizationPersistence = OrganizationPersistence(database)
    featureFlagClient = TestClient()
    secretsRepositoryReader = Mockito.mock<SecretsRepositoryReader>(SecretsRepositoryReader::class.java)
    secretsRepositoryWriter = Mockito.mock<SecretsRepositoryWriter>(SecretsRepositoryWriter::class.java)
    secretPersistenceConfigService = Mockito.mock<SecretPersistenceConfigService>(SecretPersistenceConfigService::class.java)
    dataplaneGroupService = Mockito.mock<DataplaneGroupService>(DataplaneGroupService::class.java)
    Mockito
      .`when`<DataplaneGroup?>(
        dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(
          org.mockito.kotlin.any<UUID>(),
          org.mockito.kotlin.any<String>(),
        ),
      ).thenReturn(DataplaneGroup().withId(UUID.randomUUID()))

    val metricClient = Mockito.mock<MetricClient>(MetricClient::class.java)

    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient!!,
        secretsRepositoryReader!!,
        secretsRepositoryWriter!!,
        secretPersistenceConfigService!!,
        metricClient,
      )
    truncateAllTables()

    for (organization in MockData.organizations()) {
      organizationPersistence.createOrganization(organization!!)
    }
    for (ssoConfig in MockData.ssoConfigs()) {
      organizationPersistence.createSsoConfig(ssoConfig!!)
    }
  }

  @Test
  @Throws(Exception::class)
  fun createOrganization() {
    val organization =
      Organization()
        .withOrganizationId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withEmail("octavia@airbyte.io")
        .withName("new org")
    organizationPersistence.createOrganization(organization)
    val result = organizationPersistence.getOrganization(organization.getOrganizationId())
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(organization, result.get())
  }

  @Test
  @Throws(Exception::class)
  fun createSsoConfig() {
    val org =
      Organization()
        .withOrganizationId(UUID.randomUUID())
        .withUserId(UUID.randomUUID())
        .withEmail("test@test.com")
        .withName("new org")
    val ssoConfig =
      SsoConfig()
        .withSsoConfigId(UUID.randomUUID())
        .withOrganizationId(org.getOrganizationId())
        .withKeycloakRealm("realm")
    organizationPersistence.createOrganization(org)
    organizationPersistence.createSsoConfig(ssoConfig)
    val result = organizationPersistence.getSsoConfigForOrganization(org.getOrganizationId())
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(ssoConfig, result.get())
  }

  @Test
  @Throws(Exception::class)
  fun getOrganization() {
    val result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1)
    Assertions.assertTrue(result.isPresent())
    // expecting organization 1 to have sso realm from sso config 1
    val expected = MockData.organizations().get(0)!!.withSsoRealm(MockData.ssoConfigs().get(0)!!.getKeycloakRealm())
    Assertions.assertEquals(expected, result.get())
  }

  @Test
  @Throws(Exception::class)
  fun getOrganization_notExist() {
    val result = organizationPersistence.getOrganization(UUID.randomUUID())
    Assertions.assertFalse(result.isPresent())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
  fun getOrganizationByWorkspaceId() {
    // write a workspace that belongs to org 1
    val workspace = MockData.standardWorkspaces().get(0)!!
    workspace.setOrganizationId(MockData.ORGANIZATION_ID_1)
    workspaceService.writeStandardWorkspaceNoSecrets(workspace)

    val result = organizationPersistence.getOrganizationByWorkspaceId(MockData.WORKSPACE_ID_1)
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(MockData.ORGANIZATION_ID_1, result.get()!!.getOrganizationId())
  }

  @Test
  @Throws(Exception::class)
  fun getSsoConfigForOrganization() {
    val result = organizationPersistence.getSsoConfigForOrganization(MockData.ORGANIZATION_ID_1)
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(MockData.SSO_CONFIG_ID_1, result.get()!!.getSsoConfigId())
  }

  @Test
  @Throws(Exception::class)
  fun getSsoConfigByRealmName() {
    val ssoConfig = MockData.ssoConfigs().get(0)!!
    val result = organizationPersistence.getSsoConfigByRealmName(ssoConfig.getKeycloakRealm())
    Assertions.assertTrue(result.isPresent())
    Assertions.assertEquals(ssoConfig, result.get())
  }

  @Test
  @Throws(Exception::class)
  fun updateOrganization() {
    val updatedOrganization = MockData.organizations().get(0)!!

    updatedOrganization.setName("new name")
    updatedOrganization.setEmail("newemail@airbyte.io")
    updatedOrganization.setUserId(MockData.CREATOR_USER_ID_5)

    organizationPersistence.updateOrganization(updatedOrganization)

    val result = organizationPersistence.getOrganization(MockData.ORGANIZATION_ID_1).orElseThrow()

    Assertions.assertEquals(updatedOrganization.getOrganizationId(), result.getOrganizationId())
    Assertions.assertEquals(updatedOrganization.getName(), result.getName())
    Assertions.assertEquals(updatedOrganization.getEmail(), result.getEmail())
    Assertions.assertEquals(updatedOrganization.getUserId(), result.getUserId())
  }

  @ParameterizedTest
  @CsvSource(
    "false, false",
    "true, false",
    "false, true",
    "true, true",
  )
  @Throws(Exception::class)
  fun testListOrganizationsByUserId(
    withKeywordSearch: Boolean,
    withPagination: Boolean,
  ) {
    // create a user
    val userId = UUID.randomUUID()
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(userId)
        .withName("user")
        .withAuthUserId("auth_id")
        .withEmail("email")
        .withAuthProvider(AuthProvider.AIRBYTE),
    )
    // create an org 1, name contains search "keyword"
    val orgId1 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId1)
        .withUserId(userId)
        .withName("keyword")
        .withEmail("email1"),
    )
    // grant user an admin access to org 1
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId1)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
    )
    // create an org 2, name contains search "Keyword"
    val orgId2 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId2)
        .withUserId(userId)
        .withName("Keyword")
        .withEmail("email2"),
    )
    // grant user an editor access to org 2
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId2)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_EDITOR),
    )
    // create an org 3, name contains search "randomName", owned by another user
    val orgId3 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId3)
        .withUserId(UUID.randomUUID())
        .withName("randomName")
        .withEmail("email3"),
    )
    // grant user a read access to org 3
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withOrganizationId(orgId3)
        .withUserId(userId)
        .withPermissionType(Permission.PermissionType.ORGANIZATION_READER),
    )

    var organizations: List<Organization>
    if (withKeywordSearch && withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(userId, false, 10, 0),
          Optional.of<String>("keyWord"),
        )
      // Should not contain Org with "randomName"
      Assertions.assertEquals(2, organizations.size)
    } else if (withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(userId, false, 10, 0),
          Optional.empty<String>(),
        )
      // Should also contain Org with "randomName"
      Assertions.assertEquals(3, organizations.size)
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(userId, false, 10, 2),
          Optional.empty<String>(),
        )
      // Test pagination offset
      Assertions.assertEquals(1, organizations.size)
    } else if (withKeywordSearch) { // no pagination in the request
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.of<String>("keyWord"))
      Assertions.assertEquals(2, organizations.size)
    } else { // no pagination no keyword search
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.empty<String>())
      Assertions.assertEquals(3, organizations.size)
    }
  }

  @ParameterizedTest
  @CsvSource(
    "false, false",
    "true, false",
    "false, true",
    "true, true",
  )
  @Throws(Exception::class)
  fun testListOrganizationsByUserIdForInstanceAdmin(
    withKeywordSearch: Boolean,
    withPagination: Boolean,
  ) {
    // create an instance admin user
    val instanceAdminUserId = UUID.randomUUID()
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(instanceAdminUserId)
        .withName("instance_admin")
        .withAuthUserId("instance_admin_auth_id")
        .withEmail("admin@airbyte.io")
        .withAuthProvider(AuthProvider.AIRBYTE),
    )
    // grant INSTANCE_ADMIN permission (no organization_id or workspace_id needed)
    writePermission(
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withUserId(instanceAdminUserId)
        .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN),
    )

    // create multiple organizations with different names for testing
    val orgId1 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId1)
        .withUserId(UUID.randomUUID())
        .withName("AdminKeyword")
        .withEmail("org1@test.com"),
    )

    val orgId2 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId2)
        .withUserId(UUID.randomUUID())
        .withName("AnotherKeyword")
        .withEmail("org2@test.com"),
    )

    val orgId3 = UUID.randomUUID()
    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(orgId3)
        .withUserId(UUID.randomUUID())
        .withName("NoMatch")
        .withEmail("org3@test.com"),
    )

    // total organizations in DB should be original MockData organizations + 3 new ones
    val totalExpectedOrgs = MockData.organizations().size + 3
    val keywordMatchCount = 2 // AdminKeyword and AnotherKeyword

    var organizations: List<Organization>
    if (withKeywordSearch && withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(instanceAdminUserId, false, 10, 0),
          Optional.of<String>("keyword"),
        )
      // Should contain organizations with "keyword" in the name
      Assertions.assertEquals(keywordMatchCount, organizations.size)
      // Verify they are sorted by name
      Assertions.assertTrue(organizations[0].name <= organizations[1].name)
    } else if (withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(instanceAdminUserId, false, 10, 0),
          Optional.empty<String>(),
        )
      // Should contain all organizations (including MockData ones)
      Assertions.assertEquals(totalExpectedOrgs, organizations.size)

      // Test pagination with smaller page size
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(instanceAdminUserId, false, 2, 0),
          Optional.empty<String>(),
        )
      Assertions.assertEquals(2, organizations.size)

      // Test offset
      val offsetOrganizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(instanceAdminUserId, false, 2, 2),
          Optional.empty<String>(),
        )
      Assertions.assertEquals(2, offsetOrganizations.size)
      // Should be different organizations
      Assertions.assertNotEquals(organizations[0].organizationId, offsetOrganizations[0].organizationId)
    } else if (withKeywordSearch) {
      organizations = organizationPersistence.listOrganizationsByUserId(instanceAdminUserId, Optional.of<String>("keyword"))
      Assertions.assertEquals(keywordMatchCount, organizations.size)
    } else {
      organizations = organizationPersistence.listOrganizationsByUserId(instanceAdminUserId, Optional.empty<String>())
      // Instance admin should see ALL organizations
      Assertions.assertEquals(totalExpectedOrgs, organizations.size)
    }

    // Verify that instance admin can see organizations they don't have explicit permissions on
    val allOrgIds = organizations.map { it.organizationId }
    if (!withKeywordSearch && !withPagination) {
      // Only check all organizations when not paginated (since we only get full results then)
      // Should include all MockData organizations
      for (mockOrg in MockData.organizations()) {
        Assertions.assertTrue(
          allOrgIds.contains(mockOrg!!.organizationId),
          "Instance admin should see MockData organization ${mockOrg.organizationId}",
        )
      }
      // Should include newly created organizations
      Assertions.assertTrue(allOrgIds.contains(orgId1), "Instance admin should see org1")
      Assertions.assertTrue(allOrgIds.contains(orgId2), "Instance admin should see org2")
      Assertions.assertTrue(allOrgIds.contains(orgId3), "Instance admin should see org3")
    }
  }

  @ParameterizedTest
  @CsvSource(
    "false, false",
    "true, false",
    "false, true",
    "true, true",
  )
  @Throws(Exception::class)
  fun testListOrganizationsByUserIdWithTombstoneFiltering(
    withPagination: Boolean,
    isInstanceAdmin: Boolean,
  ) {
    // Create a regular user and optionally make them instance admin
    val userId = UUID.randomUUID()
    userPersistence.writeAuthenticatedUser(
      AuthenticatedUser()
        .withUserId(userId)
        .withName("test_user")
        .withAuthUserId("test_auth_id")
        .withEmail("test@example.com")
        .withAuthProvider(AuthProvider.AIRBYTE),
    )

    if (isInstanceAdmin) {
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.INSTANCE_ADMIN),
      )
    }

    // Create 3 organizations
    val activeOrgId1 = UUID.randomUUID()
    val activeOrgId2 = UUID.randomUUID()
    val tombstonedOrgId = UUID.randomUUID()

    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(activeOrgId1)
        .withUserId(userId)
        .withName("Active Org 1")
        .withEmail("active1@test.com"),
    )

    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(activeOrgId2)
        .withUserId(userId)
        .withName("Active Org 2")
        .withEmail("active2@test.com"),
    )

    organizationPersistence.createOrganization(
      Organization()
        .withOrganizationId(tombstonedOrgId)
        .withUserId(userId)
        .withName("Tombstoned Org")
        .withEmail("tombstoned@test.com"),
    )

    if (!isInstanceAdmin) {
      // Grant user permissions to all organizations for regular user testing
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(activeOrgId1)
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
      )
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(activeOrgId2)
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
      )
      writePermission(
        Permission()
          .withPermissionId(UUID.randomUUID())
          .withOrganizationId(tombstonedOrgId)
          .withUserId(userId)
          .withPermissionType(Permission.PermissionType.ORGANIZATION_ADMIN),
      )
    }

    // Tombstone one organization by directly updating the database
    database!!.transaction<Any?> { ctx ->
      ctx
        .update(Tables.ORGANIZATION)
        .set(Tables.ORGANIZATION.TOMBSTONE, true)
        .where(Tables.ORGANIZATION.ID.eq(tombstonedOrgId))
        .execute()
      null
    }

    // Test with includeDeleted = false (default behavior)
    var organizations: List<Organization>
    if (withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(userId, false, 10, 0),
          Optional.empty<String>(),
        )
    } else {
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.empty<String>(), false)
    }

    // Should only contain non-tombstoned organizations
    val activeOrgIds = organizations.map { it.organizationId }.toSet()
    Assertions.assertTrue(activeOrgIds.contains(activeOrgId1), "Should contain active org 1")
    Assertions.assertTrue(activeOrgIds.contains(activeOrgId2), "Should contain active org 2")
    Assertions.assertFalse(activeOrgIds.contains(tombstonedOrgId), "Should NOT contain tombstoned org")

    // Test with includeDeleted = true
    if (withPagination) {
      organizations =
        organizationPersistence.listOrganizationsByUserIdPaginated(
          ResourcesByUserQueryPaginated(userId, true, 10, 0),
          Optional.empty<String>(),
        )
    } else {
      organizations = organizationPersistence.listOrganizationsByUserId(userId, Optional.empty<String>(), true)
    }

    // Should contain ALL organizations including tombstoned ones
    val allOrgIds = organizations.map { it.organizationId }.toSet()
    Assertions.assertTrue(allOrgIds.contains(activeOrgId1), "Should contain active org 1")
    Assertions.assertTrue(allOrgIds.contains(activeOrgId2), "Should contain active org 2")
    Assertions.assertTrue(allOrgIds.contains(tombstonedOrgId), "Should contain tombstoned org when includeDeleted=true")
  }
}
