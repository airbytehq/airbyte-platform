/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.AuthProvider
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Permission
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class PermissionPersistenceTest : BaseConfigDatabaseTest() {
  private var permissionPersistence: PermissionPersistence? = null
  private var organizationService: OrganizationService? = null

  @BeforeEach
  fun beforeEach() {
    permissionPersistence = PermissionPersistence(database)
    organizationService = OrganizationServiceJooqImpl(database)
    truncateAllTables()
    setupTestData()
  }

  private fun setupTestData() {
    val userPersistence = UserPersistence(database)
    val dataplaneGroupService = mockk<DataplaneGroupService>()
    every {
      dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(any<UUID>(), any<String>())
    } returns DataplaneGroup().withId(UUID.randomUUID())

    // Create organizations first (default organization is included in MockData.organizations())
    for (organization in MockData.organizations()) {
      organizationService!!.writeOrganization(organization!!)
    }

    val workspaceService: WorkspaceService =
      WorkspaceServiceJooqImpl(
        database,
        mockk<TestClient>(relaxed = true),
        mockk<SecretsRepositoryReader>(relaxed = true),
        mockk<SecretsRepositoryWriter>(relaxed = true),
        mockk<SecretPersistenceConfigService>(relaxed = true),
        mockk<MetricClient>(relaxed = true),
      )
    // write workspace table
    for (workspace in MockData.standardWorkspaces()) {
      workspaceService.writeStandardWorkspaceNoSecrets(workspace!!)
    }
    // write user table
    for (user in MockData.users()) {
      userPersistence.writeAuthenticatedUser(user!!)
    }

    // write permission table
    for (permission in MockData.permissions()) {
      writePermission(permission!!)
    }
  }

  @Test
  fun listUsersInOrganizationTest() {
    val userPermissions = permissionPersistence!!.listUsersInOrganization(MockData.ORGANIZATION_ID_1)
    Assertions.assertEquals(2, userPermissions.size)
  }

  @Test
  fun listInstanceUsersTest() {
    val userPermissions = permissionPersistence!!.listInstanceAdminUsers()
    Assertions.assertEquals(1, userPermissions.size)
    val userPermission = userPermissions.get(0)
    Assertions.assertEquals(MockData.CREATOR_USER_ID_1, userPermission.getUser().getUserId())
  }

  @Test
  fun findUsersInWorkspaceTest() {
    val permissionType =
      permissionPersistence!!
        .findPermissionTypeForUserAndWorkspace(MockData.WORKSPACE_ID_2, MockData.CREATOR_USER_ID_5.toString())
    Assertions.assertEquals(Permission.PermissionType.WORKSPACE_ADMIN, permissionType)
  }

  @Test
  fun findUsersInOrganizationTest() {
    val permissionType =
      permissionPersistence!!
        .findPermissionTypeForUserAndOrganization(MockData.ORGANIZATION_ID_2, MockData.CREATOR_USER_ID_5.toString())
    Assertions.assertEquals(Permission.PermissionType.ORGANIZATION_READER, permissionType)
  }

  @Test
  fun crossProviderIdentityRepeatPreservesWorkspaceAndOrganizationPermissionResolution() {
    val userPersistence = UserPersistence(database)
    val authUserId = MockData.CREATOR_USER_ID_5.toString()

    Assertions.assertTrue(
      userPersistence.writeAuthUser(
        MockData.CREATOR_USER_ID_5,
        authUserId,
        AuthProvider.GOOGLE_IDENTITY_PLATFORM,
      ),
    )

    Assertions.assertEquals(
      1,
      userPersistence
        .listAuthUsersForUser(MockData.CREATOR_USER_ID_5)
        .count { it.authUserId == authUserId },
    )
    Assertions.assertEquals(
      Permission.PermissionType.WORKSPACE_ADMIN,
      permissionPersistence!!.findPermissionTypeForUserAndWorkspace(MockData.WORKSPACE_ID_2, authUserId),
    )
    Assertions.assertEquals(
      Permission.PermissionType.ORGANIZATION_READER,
      permissionPersistence!!.findPermissionTypeForUserAndOrganization(MockData.ORGANIZATION_ID_2, authUserId),
    )
  }

  @Test
  fun listPermissionsForOrganizationTest() {
    val actualPermissions = permissionPersistence!!.listPermissionsForOrganization(MockData.ORGANIZATION_ID_1)
    val expectedPermissions =
      MockData
        .permissions()
        .stream()
        .filter { p: Permission? -> p!!.getOrganizationId() != null && p.getOrganizationId() == MockData.ORGANIZATION_ID_1 }
        .toList()

    Assertions.assertEquals(expectedPermissions.size, actualPermissions.size)
    for (actualPermission in actualPermissions) {
      Assertions.assertTrue(
        expectedPermissions
          .stream()
          .anyMatch { expectedPermission: Permission? ->
            expectedPermission!!.getPermissionId() == actualPermission.getPermission().getPermissionId() &&
              actualPermission.getUser().getUserId() == expectedPermission.getUserId()
          },
      )
    }
  }
}
