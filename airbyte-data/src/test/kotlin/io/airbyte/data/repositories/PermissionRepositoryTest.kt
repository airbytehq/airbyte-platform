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
import org.junit.jupiter.api.Test
import java.util.UUID

@MicronautTest
internal class PermissionRepositoryTest : AbstractConfigRepositoryTest<PermissionRepository>(PermissionRepository::class) {
  companion object {
    @BeforeAll
    @JvmStatic
    fun setup() {
      // so we don't have to deal with making users/workspaces as well
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_USER_ID_FKEY.constraint()).execute()
      jooqDslContext.alterTable(Tables.PERMISSION).dropForeignKey(Keys.PERMISSION__PERMISSION_WORKSPACE_ID_FKEY.constraint()).execute()
    }
  }

  @Test
  fun `test db insertion and find`() {
    val permission =
      Permission(
        workspaceId = UUID.randomUUID(),
        userId = UUID.randomUUID(),
        permissionType = PermissionType.workspace_admin,
      )

    val countBeforeSave = repository.count()

    val saveResult = repository.save(permission)

    assertEquals(countBeforeSave + 1, repository.count())

    val persistedPermission = repository.findById(saveResult.id!!).get()

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
}
