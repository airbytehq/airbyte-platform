/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Permission
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.util.UUID

internal class PermissionPersistenceTest : BaseConfigDatabaseTest() {
  private var permissionPersistence: PermissionPersistence? = null
  private var organizationPersistence: OrganizationPersistence? = null

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    permissionPersistence = PermissionPersistence(database)
    organizationPersistence = OrganizationPersistence(database)
    truncateAllTables()
    setupTestData()
  }

  @Throws(Exception::class)
  private fun setupTestData() {
    val userPersistence = UserPersistence(database)
    val dataplaneGroupService = Mockito.mock<DataplaneGroupService>(DataplaneGroupService::class.java)
    Mockito
      .`when`<DataplaneGroup?>(
        dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(
          org.mockito.kotlin.any<UUID>(),
          org.mockito.kotlin.any<String>(),
        ),
      ).thenReturn(DataplaneGroup().withId(UUID.randomUUID()))

    // Create organizations first (default organization is included in MockData.organizations())
    for (organization in MockData.organizations()) {
      organizationPersistence!!.createOrganization(organization!!)
    }

    val workspaceService: WorkspaceService =
      WorkspaceServiceJooqImpl(
        database,
        Mockito.mock<TestClient?>(TestClient::class.java),
        Mockito.mock<SecretsRepositoryReader?>(SecretsRepositoryReader::class.java),
        Mockito.mock<SecretsRepositoryWriter?>(SecretsRepositoryWriter::class.java),
        Mockito.mock<SecretPersistenceConfigService?>(SecretPersistenceConfigService::class.java),
        Mockito.mock<MetricClient?>(MetricClient::class.java),
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
  @Throws(IOException::class)
  fun listUsersInOrganizationTest() {
    val userPermissions = permissionPersistence!!.listUsersInOrganization(MockData.ORGANIZATION_ID_1)
    Assertions.assertEquals(2, userPermissions.size)
  }

  @Test
  @Throws(IOException::class)
  fun listInstanceUsersTest() {
    val userPermissions = permissionPersistence!!.listInstanceAdminUsers()
    Assertions.assertEquals(1, userPermissions.size)
    val userPermission = userPermissions.get(0)
    Assertions.assertEquals(MockData.CREATOR_USER_ID_1, userPermission.getUser().getUserId())
  }

  @Test
  @Throws(Exception::class)
  fun findUsersInWorkspaceTest() {
    val permissionType =
      permissionPersistence!!
        .findPermissionTypeForUserAndWorkspace(MockData.WORKSPACE_ID_2, MockData.CREATOR_USER_ID_5.toString())
    Assertions.assertEquals(Permission.PermissionType.WORKSPACE_ADMIN, permissionType)
  }

  @Test
  @Throws(Exception::class)
  fun findUsersInOrganizationTest() {
    val permissionType =
      permissionPersistence!!
        .findPermissionTypeForUserAndOrganization(MockData.ORGANIZATION_ID_2, MockData.CREATOR_USER_ID_5.toString())
    Assertions.assertEquals(Permission.PermissionType.ORGANIZATION_READER, permissionType)
  }

  @Test
  @Throws(Exception::class)
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
