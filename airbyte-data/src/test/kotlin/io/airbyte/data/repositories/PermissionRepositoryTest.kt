/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.commons.enums.toEnum
import io.airbyte.data.repositories.entities.DataplaneGroup
import io.airbyte.data.repositories.entities.Group
import io.airbyte.data.repositories.entities.GroupMember
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.Status
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class PermissionRepositoryTest : AbstractConfigRepositoryTest() {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making users/workspaces/orgs as well
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_USER_ID_FKEY.constraint()).execute()
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_WORKSPACE_ID_FKEY.constraint()).execute()
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_ORGANIZATION_ID_FKEY.constraint()).execute()
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_SERVICE_ACCOUNT_ID_FKEY.constraint()).execute()
    }
  }

  @BeforeEach
  fun setupEach() {
    permissionRepository.deleteAll()
  }

  @Test
  fun `test db insertion and find`() {
    val permission =
      Permission(
        workspaceId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.workspace_admin,
      )

    val countBeforeSave = permissionRepository.count()

    val saveResult = permissionRepository.save(permission)

    assertEquals(countBeforeSave + 1, permissionRepository.count())

    val persistedPermission = permissionRepository.findById(saveResult.id!!).get()

    with(persistedPermission) {
      assertEquals(id, saveResult.id)
      assertEquals(workspaceId, permission.workspaceId)
      assertEquals(userId, permission.userId)
      assertEquals(permissionType, permission.permissionType)
      assertNull(organizationId)
      assertNotNull(createdAt)
      assertNotNull(updatedAt)
    }
  }

  @Test
  fun `test findByIdIn`() {
    val permission1 =
      Permission(id = UUID.randomUUID(), workspaceId = UUID.randomUUID(), userId = UUID.randomUUID(), permissionType = PermissionType.workspace_admin)
    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.workspace_reader,
      )
    val permission3 =
      Permission(id = UUID.randomUUID(), workspaceId = UUID.randomUUID(), userId = UUID.randomUUID(), permissionType = PermissionType.workspace_admin)

    permissionRepository.save(permission1)
    permissionRepository.save(permission2)
    permissionRepository.save(permission3)

    val result = permissionRepository.findByIdIn(listOf(permission1.id!!, permission3.id!!))

    assertEquals(2, result.size)
    assertEquals(setOf(permission1.id, permission3.id), result.map { it.id }.toSet())
  }

  @Test
  fun `test findByUserId`() {
    val userId = UUID.randomUUID()

    val permission1 = Permission(workspaceId = UUID.randomUUID(), userId = userId, permissionType = PermissionType.workspace_admin)
    val permission2 = Permission(workspaceId = UUID.randomUUID(), userId = userId, permissionType = PermissionType.workspace_reader)
    val permission3 =
      Permission(
        workspaceId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.workspace_admin,
      ) // different user

    permissionRepository.save(permission1)
    permissionRepository.save(permission2)
    permissionRepository.save(permission3)

    val result = permissionRepository.findByUserId(userId)

    assertEquals(2, result.size)
    assertEquals(setOf(permission1.id, permission2.id), result.map { it.id }.toSet())
  }

  @Test
  fun `test findByOrganizationId`() {
    val organizationId = UUID.randomUUID()

    val permission1 = Permission(organizationId = organizationId, userId = UUID.randomUUID(), permissionType = PermissionType.organization_admin)
    val permission2 = Permission(organizationId = organizationId, userId = UUID.randomUUID(), permissionType = PermissionType.organization_editor)
    val permission3 =
      Permission(
        organizationId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.organization_admin,
      ) // different org

    permissionRepository.save(permission1)
    permissionRepository.save(permission2)
    permissionRepository.save(permission3)

    val result = permissionRepository.findByOrganizationId(organizationId)

    assertEquals(2, result.size)
    assertEquals(setOf(permission1.id, permission2.id), result.map { it.id }.toSet())
  }

  @Test
  fun `test deleteByIdIn`() {
    val permission1 =
      Permission(id = UUID.randomUUID(), workspaceId = UUID.randomUUID(), userId = UUID.randomUUID(), permissionType = PermissionType.workspace_admin)
    val permission2 =
      Permission(
        id = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.workspace_reader,
      )
    val permission3 =
      Permission(id = UUID.randomUUID(), workspaceId = UUID.randomUUID(), userId = UUID.randomUUID(), permissionType = PermissionType.workspace_admin)

    permissionRepository.save(permission1)
    permissionRepository.save(permission2)
    permissionRepository.save(permission3)

    assertEquals(3, permissionRepository.count())

    permissionRepository.deleteByIdIn(listOf(permission1.id!!, permission2.id!!))

    assertEquals(1, permissionRepository.count())
    assertEquals(permission3.id, permissionRepository.findAll().first().id)
  }

  @Test
  fun `findByServiceAccountId returns a permission based on service account id when an org id is provided`() {
    val serviceAccountId = UUID.randomUUID()
    val permission =
      Permission(
        id = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        serviceAccountId = serviceAccountId,
        permissionType = PermissionType.dataplane,
      )

    permissionRepository.save(permission)

    val result = permissionRepository.findByServiceAccountId(serviceAccountId)
    assertEquals(1, result.size)

    val returned = result.first()
    assertEquals(permission.id, returned.id)
    assertEquals(permission.serviceAccountId, returned.serviceAccountId)
    assertEquals(permission.permissionType, returned.permissionType)
    assertEquals(permission.organizationId, returned.organizationId)
    assertNull(returned.workspaceId)
    assertNull(returned.userId)
  }

  @Test
  fun `findByServiceAccountId returns a permission based on service account id when org id and workspace id are null`() {
    val serviceAccountId = UUID.randomUUID()
    val permission =
      Permission(
        id = UUID.randomUUID(),
        serviceAccountId = serviceAccountId,
        permissionType = PermissionType.dataplane,
      )

    permissionRepository.save(permission)

    val result = permissionRepository.findByServiceAccountId(serviceAccountId)
    assertEquals(1, result.size)

    val returned = result.first()
    assertEquals(permission.id, returned.id)
    assertEquals(permission.serviceAccountId, returned.serviceAccountId)
    assertEquals(permission.permissionType, returned.permissionType)
    assertNull(returned.organizationId)
    assertNull(returned.workspaceId)
    assertNull(returned.userId)
  }

  @Test
  fun `queryByAuthUser returns direct user permissions`() {
    val userId = createUserWithAuthUser("direct-user-auth-id")
    val directPermission =
      permissionRepository.save(
        Permission(
          id = UUID.randomUUID(),
          organizationId = UUID.randomUUID(),
          userId = userId,
          permissionType = PermissionType.organization_admin,
        ),
      )
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        organizationId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.organization_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("direct-user-auth-id")

    assertEquals(setOf(directPermission.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser returns group organization permissions in the group organization`() {
    val userId = createUserWithAuthUser("group-org-user-auth-id")
    val org = createOrganization()
    val group = createGroup(org.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, org.id!!)
    val groupPermission =
      permissionRepository.save(
        Permission(
          id = UUID.randomUUID(),
          organizationId = org.id,
          groupId = group.id,
          permissionType = PermissionType.organization_admin,
        ),
      )

    val result = permissionRepository.queryByAuthUser("group-org-user-auth-id")

    assertEquals(setOf(organizationMembership.id, groupPermission.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser ignores group organization permissions outside the group organization`() {
    val userId = createUserWithAuthUser("cross-org-group-org-user-auth-id")
    val groupOrg = createOrganization(name = "group org")
    val otherOrg = createOrganization(name = "other org")
    val group = createGroup(groupOrg.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, groupOrg.id!!)
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        organizationId = otherOrg.id,
        groupId = group.id,
        permissionType = PermissionType.organization_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("cross-org-group-org-user-auth-id")

    assertEquals(setOf(organizationMembership.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser ignores group permissions when user is only a member of another organization`() {
    val userId = createUserWithAuthUser("non-member-group-user-auth-id")
    val org = createOrganization()
    val otherOrg = createOrganization()
    val group = createGroup(org.id!!)
    createGroupMember(group.id!!, userId)
    val otherOrganizationMembership = createOrganizationMembershipPermission(userId, otherOrg.id!!)
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        organizationId = org.id,
        groupId = group.id,
        permissionType = PermissionType.organization_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("non-member-group-user-auth-id")

    assertEquals(setOf(otherOrganizationMembership.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser returns group workspace permissions in the group organization`() {
    val userId = createUserWithAuthUser("group-workspace-user-auth-id")
    val org = createOrganization()
    val group = createGroup(org.id!!)
    val workspace = createWorkspace(org.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, org.id!!)
    val groupPermission =
      permissionRepository.save(
        Permission(
          id = UUID.randomUUID(),
          workspaceId = workspace.id,
          groupId = group.id,
          permissionType = PermissionType.workspace_admin,
        ),
      )

    val result = permissionRepository.queryByAuthUser("group-workspace-user-auth-id")

    assertEquals(setOf(organizationMembership.id, groupPermission.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser ignores group workspace permissions outside the group organization`() {
    val userId = createUserWithAuthUser("cross-org-group-workspace-user-auth-id")
    val groupOrg = createOrganization(name = "group org")
    val workspaceOrg = createOrganization(name = "workspace org")
    val group = createGroup(groupOrg.id!!)
    val workspace = createWorkspace(workspaceOrg.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, groupOrg.id!!)
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        workspaceId = workspace.id,
        groupId = group.id,
        permissionType = PermissionType.workspace_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("cross-org-group-workspace-user-auth-id")

    assertEquals(setOf(organizationMembership.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser ignores permissions with mixed user and group ownership`() {
    val userId = createUserWithAuthUser("mixed-owner-user-auth-id")
    val org = createOrganization()
    val group = createGroup(org.id!!)
    val workspace = createWorkspace(org.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, org.id!!)
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        groupId = group.id,
        workspaceId = workspace.id,
        permissionType = PermissionType.workspace_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("mixed-owner-user-auth-id")

    assertEquals(setOf(organizationMembership.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser ignores group permissions whose type does not match their scope`() {
    val userId = createUserWithAuthUser("mismatched-scope-user-auth-id")
    val org = createOrganization()
    val group = createGroup(org.id!!)
    val workspace = createWorkspace(org.id!!)
    createGroupMember(group.id!!, userId)
    val organizationMembership = createOrganizationMembershipPermission(userId, org.id!!)
    permissionRepository.saveAll(
      listOf(
        Permission(
          id = UUID.randomUUID(),
          groupId = group.id,
          organizationId = org.id,
          permissionType = PermissionType.workspace_admin,
        ),
        Permission(
          id = UUID.randomUUID(),
          groupId = group.id,
          workspaceId = workspace.id,
          permissionType = PermissionType.organization_admin,
        ),
      ),
    )

    val result = permissionRepository.queryByAuthUser("mismatched-scope-user-auth-id")

    assertEquals(setOf(organizationMembership.id), result.map { it.id }.toSet())
  }

  @Test
  fun `queryByAuthUser requires an organization permission type for membership`() {
    val userId = createUserWithAuthUser("invalid-membership-user-auth-id")
    val org = createOrganization()
    val group = createGroup(org.id!!)
    createGroupMember(group.id!!, userId)
    val invalidOrganizationMembership =
      permissionRepository.save(
        Permission(
          id = UUID.randomUUID(),
          userId = userId,
          organizationId = org.id,
          permissionType = PermissionType.workspace_admin,
        ),
      )
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        groupId = group.id,
        organizationId = org.id,
        permissionType = PermissionType.organization_admin,
      ),
    )

    val result = permissionRepository.queryByAuthUser("invalid-membership-user-auth-id")

    assertEquals(setOf(invalidOrganizationMembership.id), result.map { it.id }.toSet())
  }

  private fun createUserWithAuthUser(authUserId: String): UUID {
    val userId = UUID.randomUUID()
    jooqDslContext
      .insertInto(Tables.USER)
      .set(Tables.USER.ID, userId)
      .set(Tables.USER.NAME, "Test User $authUserId")
      .set(Tables.USER.EMAIL, "$authUserId@example.com")
      .set(Tables.USER.STATUS, "registered".toEnum<Status>()!!)
      .execute()
    jooqDslContext
      .insertInto(Tables.AUTH_USER)
      .set(Tables.AUTH_USER.ID, UUID.randomUUID())
      .set(Tables.AUTH_USER.USER_ID, userId)
      .set(Tables.AUTH_USER.AUTH_USER_ID, authUserId)
      .set(Tables.AUTH_USER.AUTH_PROVIDER, "airbyte".toEnum<AuthProvider>()!!)
      .execute()
    return userId
  }

  private fun createOrganizationMembershipPermission(
    userId: UUID,
    organizationId: UUID,
  ): Permission =
    permissionRepository.save(
      Permission(
        id = UUID.randomUUID(),
        userId = userId,
        organizationId = organizationId,
        permissionType = PermissionType.organization_member,
      ),
    )

  private fun createOrganization(name: String = "Test Org ${UUID.randomUUID()}"): Organization =
    organizationRepository.save(
      Organization(
        name = name,
        email = "${UUID.randomUUID()}@example.com",
      ),
    )

  private fun createGroup(organizationId: UUID): Group =
    context.getBean(GroupRepository::class.java).save(
      Group(
        name = "Test Group ${UUID.randomUUID()}",
        organizationId = organizationId,
      ),
    )

  private fun createGroupMember(
    groupId: UUID,
    userId: UUID,
  ): GroupMember =
    context.getBean(GroupMemberRepository::class.java).save(
      GroupMember(
        groupId = groupId,
        userId = userId,
      ),
    )

  private fun createWorkspace(organizationId: UUID): Workspace =
    dataplaneGroupRepository
      .save(
        DataplaneGroup(
          organizationId = organizationId,
          name = "Test Dataplane Group ${UUID.randomUUID()}",
          enabled = true,
          tombstone = false,
        ),
      ).let { dataplaneGroup ->
        workspaceRepository.save(
          Workspace(
            customerId = UUID.randomUUID(),
            name = "Test Workspace",
            slug = "test-workspace-${UUID.randomUUID()}",
            email = "test@example.com",
            dataplaneGroupId = dataplaneGroup.id!!,
            organizationId = organizationId,
          ),
        )
      }
}
