/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
class V1_6_0_010__Add_Connector_IPC_Options_ColumnTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V1_6_0_009__DropDefaultGeographyFromConnectionTemplate",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V1_6_0_007__ScopeTemplatesByActorDefinition()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  fun testConnectorIpcOptionsColumnMigration() {
    val ctx = getDslContext()
    val beforeMigration =
      ctx.fetchOne(
        """
        SELECT is_nullable, data_type
        FROM information_schema.columns
        WHERE table_name = 'actor_definition_version'
          AND column_name = 'connector_ipc_options'
        """.trimIndent(),
      )
    assertNull(beforeMigration, "connector_ipc_options should not exist before migration")

    V1_6_0_010__Add_Connector_IPC_Options_Column.addIPCOptionsColumn(dslContext)

    val afterMigration =
      ctx.fetchOne(
        """
        SELECT is_nullable, data_type
        FROM information_schema.columns
        WHERE table_name = 'actor_definition_version'
          AND column_name = 'connector_ipc_options'
        """.trimIndent(),
      )!!

    assertNotNull(afterMigration, "connector_ipc_options should exist after migration")
    assertEquals("YES", afterMigration.get("is_nullable", String::class.java), "Column should be nullable")
    assertEquals("jsonb", afterMigration.get("data_type", String::class.java), "Column should be of type JSONB")
  }
}
