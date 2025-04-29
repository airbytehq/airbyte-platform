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
internal class V1_1_1_031__AllowNullPartialUserConfigConfigurationTest : AbstractConfigsDatabaseTest() {
  companion object {
    private val PARTIAL_USER_CONFIG_TABLE = DSL.table("partial_user_config")
    private val ID_FIELD = DSL.field("id", java.util.UUID::class.java)
    private val WORKSPACE_ID_FIELD = DSL.field("workspace_id", UUID::class.java)
    private val CONFIG_TEMPLATE_ID_FIELD = DSL.field("config_template_id", UUID::class.java)
    private val ACTOR_ID = DSL.field("actor_id", UUID::class.java)
  }

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_1_1_031__AllowNullPartialUserConfigConfigurationTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)
    val previousMigration: BaseJavaMigration = V1_1_1_030__BackfillFiltersUpdate()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun `allows null partial_user_config_properties column now`() {
    val ctx = getDslContext()

    // Run the migration
    V1_1_1_031__AllowNullPartialUserConfigConfiguration.dropNotNullFromConfigurationColumn(ctx)

    ctx.execute("ALTER TABLE partial_user_config DROP CONSTRAINT IF EXISTS partial_user_config_actor_id_fkey")
    ctx.execute("ALTER TABLE partial_user_config DROP CONSTRAINT IF EXISTS partial_user_config_template_id_fkey")
    ctx.execute("ALTER TABLE partial_user_config DROP CONSTRAINT IF EXISTS partial_user_config_workspace_id_fkey")

    // Insert a row with null configuration
    ctx
      .insertInto(PARTIAL_USER_CONFIG_TABLE)
      .set(ID_FIELD, UUID.randomUUID())
      .set(WORKSPACE_ID_FIELD, UUID.randomUUID())
      .set(CONFIG_TEMPLATE_ID_FIELD, UUID.randomUUID())
      .set(ACTOR_ID, UUID.randomUUID())
      .execute()

    // Verify the row was inserted successfully
    val result = ctx.selectFrom(PARTIAL_USER_CONFIG_TABLE).fetch()

    assertNotNull(result)
    assertTrue(result.isNotEmpty)
  }
}
