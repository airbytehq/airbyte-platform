/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Table
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_24_007__AddUniqueConstraintInUserTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_24_007__AddUniqueConstraintInUserTableTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testMigrate() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val authUserId1 = "authUserId1"
    val authUserId2 = "authUserId2"
    val authUserId3 = "authUserId3"

    val authProvider1 =
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider.GOOGLE_IDENTITY_PLATFORM
    val authProvider2 = V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider.AIRBYTE
    val authProvider3 = V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider.KEYCLOAK

    // Set up a state with multiple versions and some duplicates
    insertUser(ctx, authUserId1, authProvider1)
    insertUser(ctx, authUserId1, authProvider1)
    insertUser(ctx, authUserId1, authProvider2)

    insertUser(ctx, authUserId2, authProvider1)
    insertUser(ctx, authUserId2, authProvider2)
    insertUser(ctx, authUserId2, authProvider3)

    insertUser(ctx, authUserId3, authProvider1)
    insertUser(ctx, authUserId3, authProvider2)
    insertUser(ctx, authUserId3, authProvider3)
    insertUser(ctx, authUserId3, authProvider3)

    // Initial assertions
    assertUserCount(ctx, authUserId1, 3)
    assertUserCount(ctx, authUserId2, 3)
    assertUserCount(ctx, authUserId3, 4)

    assertUserTagCount(ctx, authUserId1, authProvider1, 2)
    assertUserTagCount(ctx, authUserId1, authProvider2, 1)
    assertUserTagCount(ctx, authUserId2, authProvider1, 1)
    assertUserTagCount(ctx, authUserId2, authProvider2, 1)
    assertUserTagCount(ctx, authUserId2, authProvider3, 1)
    assertUserTagCount(ctx, authUserId3, authProvider1, 1)
    assertUserTagCount(ctx, authUserId3, authProvider2, 1)
    assertUserTagCount(ctx, authUserId3, authProvider3, 2)

    // Run migration
    V0_50_24_007__AddUniqueConstraintInUserTable.migrate(ctx)

    // Assert duplicate rows were dropped
    assertUserCount(ctx, authUserId1, 2)
    assertUserCount(ctx, authUserId2, 3)
    assertUserCount(ctx, authUserId3, 3)

    assertUserTagCount(ctx, authUserId1, authProvider1, 1)
    assertUserTagCount(ctx, authUserId1, authProvider2, 1)
    assertUserTagCount(ctx, authUserId2, authProvider1, 1)
    assertUserTagCount(ctx, authUserId2, authProvider2, 1)
    assertUserTagCount(ctx, authUserId2, authProvider3, 1)
    assertUserTagCount(ctx, authUserId3, authProvider1, 1)
    assertUserTagCount(ctx, authUserId3, authProvider2, 1)
    assertUserTagCount(ctx, authUserId3, authProvider3, 1)

    // Attempting to re-insert an existing row should now fail
    Assertions.assertThrows(
      DataAccessException::class.java,
    ) { insertUser(ctx, authUserId1, authProvider1) }
  }

  companion object {
    val USER_TABLE: Table<*> = DSL.table("\"user\"")
    val AUTH_USER_ID_FIELD: Field<String> =
      DSL.field(
        "auth_user_id",
        String::class.java,
      )
    val AUTH_PROVIDER_FIELD: Field<V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider> =
      DSL.field(
        "auth_provider",
        V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java,
      )

    private fun insertUser(
      ctx: DSLContext,
      authUserId: String,
      authProvider: V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider,
    ) {
      ctx
        .insertInto(USER_TABLE)
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("email"),
          AUTH_USER_ID_FIELD,
          AUTH_PROVIDER_FIELD,
        ).values(
          UUID.randomUUID(),
          "user name",
          "user@email.com",
          authUserId,
          authProvider,
        ).execute()
    }

    private fun assertUserCount(
      ctx: DSLContext,
      authUserId: String,
      expectedCount: Int,
    ) {
      val actualCount =
        ctx
          .select()
          .from(USER_TABLE)
          .where(AUTH_USER_ID_FIELD.eq(authUserId))
          .fetch()
          .size
      Assertions.assertEquals(expectedCount, actualCount)
    }

    private fun assertUserTagCount(
      ctx: DSLContext,
      authUserId: String,
      authProvider: V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider,
      expectedCount: Int,
    ) {
      val actualCount =
        ctx
          .select()
          .from(USER_TABLE)
          .where(AUTH_USER_ID_FIELD.eq(authUserId))
          .and(AUTH_PROVIDER_FIELD.eq(authProvider))
          .fetch()
          .size
      Assertions.assertEquals(expectedCount, actualCount)
    }
  }
}
