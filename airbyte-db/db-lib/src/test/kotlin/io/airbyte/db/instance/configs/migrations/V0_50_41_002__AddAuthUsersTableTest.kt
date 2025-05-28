/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.DatabaseConstants.AUTH_USER_TABLE
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_41_002__AddAuthUsersTable.Companion.createAuthUsersTable
import io.airbyte.db.instance.configs.migrations.V0_50_41_002__AddAuthUsersTable.Companion.populateAuthUserTable
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_41_002__AddAuthUsersTableTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_33_018__AddAuthUsersTable",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_33_016__AddIconUrlToActorDefinition()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun testPopulateAuthUsersTable() {
    val context = getDslContext()

    createAuthUsersTable(context)
    populateAuthUserTable(context)

    Assertions.assertTrue(authUserRowForDefaultUserExists(context))
  }

  companion object {
    private const val EXPECTED_DEFAULT_USER_AUTH_USER_ID = "00000000-0000-0000-0000-000000000000"
    private val EXPECTED_DEFAULT_USER_AUTH_PROVIDER =
      V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider.AIRBYTE

    fun authUserRowForDefaultUserExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from(AUTH_USER_TABLE)
          .where(
            DSL.field<String>("auth_user_id", String::class.java).eq(
              EXPECTED_DEFAULT_USER_AUTH_USER_ID,
            ),
          ).and(
            DSL
              .field<V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider>(
                "auth_provider",
                SQLDataType.VARCHAR.asEnumDataType<V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider>(
                  V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java,
                ),
              ).eq(
                EXPECTED_DEFAULT_USER_AUTH_PROVIDER,
              ),
          ),
      )
  }
}
