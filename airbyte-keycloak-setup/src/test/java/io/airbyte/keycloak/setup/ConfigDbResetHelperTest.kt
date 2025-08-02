/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.keycloak.setup

import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.jooq.impl.TableImpl
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.util.UUID

internal class ConfigDbResetHelperTest : BaseConfigDatabaseTest() {
  private lateinit var configDbResetHelper: ConfigDbResetHelper

  @BeforeEach
  @Throws(Exception::class)
  fun setUp() {
    configDbResetHelper = ConfigDbResetHelper(database!!)
    truncateAllTables()

    // Pre-populate the database with test data
    database!!.transaction { ctx: DSLContext ->
      ctx
        .insertInto(
          Tables.ORGANIZATION,
          Tables.ORGANIZATION.ID,
          Tables.ORGANIZATION.NAME,
          Tables.ORGANIZATION.EMAIL,
        ).values(ORGANIZATION_ID, "Org", "org@airbyte.io")
        .execute()
      // Insert sample users
      ctx
        .insertInto(Tables.USER, Tables.USER.ID, Tables.USER.EMAIL, Tables.USER.NAME)
        .values(KEYCLOAK_USER_1_ID, "one@airbyte.io", "User One")
        .values(KEYCLOAK_USER_2_ID, "two@airbyte.io", "User Two")
        .values(NON_KEYCLOAK_USER_ID, "three@airbyte.io", "User Three")
        .execute()

      // Insert auth users for these users
      ctx
        .insertInto(
          Tables.AUTH_USER,
          Tables.AUTH_USER.ID,
          Tables.AUTH_USER.USER_ID,
          Tables.AUTH_USER.AUTH_USER_ID,
          Tables.AUTH_USER.AUTH_PROVIDER,
        ).values(UUID.randomUUID(), KEYCLOAK_USER_1_ID, UUID.randomUUID().toString(), AuthProvider.keycloak)
        .values(UUID.randomUUID(), KEYCLOAK_USER_2_ID, UUID.randomUUID().toString(), AuthProvider.keycloak)
        .values(UUID.randomUUID(), NON_KEYCLOAK_USER_ID, UUID.randomUUID().toString(), AuthProvider.airbyte)
        .execute()

      // Insert permissions for these users
      ctx
        .insertInto(
          Tables.PERMISSION,
          Tables.PERMISSION.ID,
          Tables.PERMISSION.USER_ID,
          Tables.PERMISSION.ORGANIZATION_ID,
          Tables.PERMISSION.PERMISSION_TYPE,
        ).values(UUID.randomUUID(), KEYCLOAK_USER_1_ID, ORGANIZATION_ID, PermissionType.organization_admin)
        .values(UUID.randomUUID(), KEYCLOAK_USER_2_ID, ORGANIZATION_ID, PermissionType.organization_member)
        .values(UUID.randomUUID(), NON_KEYCLOAK_USER_ID, null, PermissionType.instance_admin)
        .execute()
      null
    }
  }

  @Test
  @Throws(Exception::class)
  fun throwsIfMultipleOrgsDetected() {
    // Insert a second organization
    database!!.query { ctx: DSLContext ->
      ctx
        .insertInto(
          Tables.ORGANIZATION,
          Tables.ORGANIZATION.ID,
          Tables.ORGANIZATION.NAME,
          Tables.ORGANIZATION.EMAIL,
        ).values(UUID.randomUUID(), "Org 2", "org2@airbyte.io")
        .execute()
      null
    }

    // Expect an exception to be thrown when the helper is invoked
    Assertions.assertThrows(IllegalStateException::class.java) { configDbResetHelper.deleteConfigDbUsers() }

    // Expect no records to be deleted
    Assertions.assertEquals(3, countRowsInTable(Tables.USER))
    Assertions.assertEquals(3, countRowsInTable(Tables.PERMISSION))
  }

  @Test
  @Throws(SQLException::class)
  fun deleteConfigDbUsers_KeycloakUsersExist_UsersAndPermissionsDeleted() {
    // Before deletion, assert the initial state of the database
    Assertions.assertEquals(3, countRowsInTable(Tables.USER))
    Assertions.assertEquals(3, countRowsInTable(Tables.PERMISSION))

    // Perform the deletion operation
    configDbResetHelper.deleteConfigDbUsers()

    // Assert the state of the database after deletion
    // Expecting users with AuthProvider keycloak and their permissions to be deleted
    Assertions.assertEquals(1, countRowsInTable(Tables.USER))
    Assertions.assertEquals(1, countRowsInTable(Tables.PERMISSION))

    // Assert that the remaining user is the one not backed by keycloak
    val remainingUserAuthProvider =
      database!!.query { ctx: DSLContext ->
        ctx
          .select(Tables.AUTH_USER.AUTH_PROVIDER)
          .from(Tables.AUTH_USER)
          .fetchOne(Tables.AUTH_USER.AUTH_PROVIDER)
      }
    Assertions.assertEquals(AuthProvider.airbyte, remainingUserAuthProvider)

    // Assert that the remaining permission is the one not associated with a keycloak user
    val remainingPermissionType =
      database!!.query { ctx: DSLContext ->
        ctx
          .select(Tables.PERMISSION.PERMISSION_TYPE)
          .from(Tables.PERMISSION)
          .fetchOne(Tables.PERMISSION.PERMISSION_TYPE)
      }
    Assertions.assertEquals(PermissionType.instance_admin, remainingPermissionType)
  }

  @Throws(SQLException::class)
  private fun countRowsInTable(table: TableImpl<*>): Int =
    database!!.query { ctx: DSLContext ->
      ctx.selectCount().from(table).fetchOne(0, Int::class.javaPrimitiveType)!!
    }

  companion object {
    private val KEYCLOAK_USER_1_ID: UUID = UUID.randomUUID()
    private val KEYCLOAK_USER_2_ID: UUID = UUID.randomUUID()
    private val NON_KEYCLOAK_USER_ID: UUID = UUID.randomUUID()
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
  }
}
