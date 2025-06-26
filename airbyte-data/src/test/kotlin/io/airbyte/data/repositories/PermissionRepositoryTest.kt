/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Permission
import io.airbyte.db.instance.configs.jooq.generated.Keys
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
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
}
