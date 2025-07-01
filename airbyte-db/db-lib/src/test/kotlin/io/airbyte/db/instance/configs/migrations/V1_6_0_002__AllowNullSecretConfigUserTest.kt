/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_6_0_002__AllowNullSecretConfigUserTest : AbstractConfigsDatabaseTest() {
  companion object {
    private val SECRET_CONFIG = DSL.table("secret_config")
    private val ID_FIELD = DSL.field("id", UUID::class.java)
    private val SECRET_STORAGE_ID_FIELD = DSL.field("secret_storage_id", UUID::class.java)
    private val DESCRIPTOR_FIELD = DSL.field("descriptor", String::class.java)
    private val EXTERNAL_COORDINATE_FIELD = DSL.field("external_coordinate", String::class.java)
    private val AIRBYTE_MANAGED_FIELD = DSL.field("airbyte_managed", Boolean::class.java)
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_1_1_032__AllowNullSecretConfigUserTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_031__AllowNullPartialUserConfigConfiguration()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `allows null created_by and updated_by column values now`() {
    val ctx = dslContext!!

    // Run the migration
    V1_6_0_002__AllowNullSecretConfigUser.dropExtraForeignKeyConstraint(ctx)
    V1_6_0_002__AllowNullSecretConfigUser.dropNotNullFromSecretConfigUserColumns(ctx)

    ctx.execute("ALTER TABLE secret_config DROP CONSTRAINT IF EXISTS secret_config_secret_storage_id_fkey")

    // Insert a row with null creator / updater user
    ctx
      .insertInto(SECRET_CONFIG)
      .set(ID_FIELD, UUID.randomUUID())
      .set(SECRET_STORAGE_ID_FIELD, UUID.randomUUID())
      .set(DESCRIPTOR_FIELD, "test_descriptor")
      .set(EXTERNAL_COORDINATE_FIELD, "test_external_coordinate")
      .set(AIRBYTE_MANAGED_FIELD, true)
      .execute()

    // Verify the row was inserted successfully
    val result = ctx.selectFrom(SECRET_CONFIG).fetch()

    assertNotNull(result)
    assertTrue(result.isNotEmpty)
  }
}
