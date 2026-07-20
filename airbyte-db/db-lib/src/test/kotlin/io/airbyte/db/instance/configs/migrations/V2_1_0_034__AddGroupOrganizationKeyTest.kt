/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_034__AddGroupOrganizationKeyTest : AbstractConfigsDatabaseTest() {
  @Test
  fun `adds the composite Group organization key`() {
    val ctx = baselineToPreviousMigration()

    assertNull(constraintColumns(ctx))

    V2_1_0_034__AddGroupOrganizationKey.addGroupOrganizationKey(ctx)

    assertEquals("id,organization_id", constraintColumns(ctx))
    ctx.execute(
      """
      CREATE TABLE group_organization_reference_test (
        group_id UUID NOT NULL,
        organization_id UUID NOT NULL,
        FOREIGN KEY (group_id, organization_id)
          REFERENCES "group"(id, organization_id)
      )
      """.trimIndent(),
    )
    ctx.execute("DROP TABLE group_organization_reference_test")
  }

  private fun baselineToPreviousMigration(): DSLContext {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_034__AddGroupOrganizationKeyTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_033__DropOrchestrationTables()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    return dslContext!!
  }

  private fun constraintColumns(ctx: DSLContext): String? =
    ctx.fetchValue(
      """
      SELECT string_agg(attribute_metadata.attname, ',' ORDER BY constraint_columns.ordinality)
      FROM pg_constraint constraint_metadata
      JOIN pg_class table_metadata ON table_metadata.oid = constraint_metadata.conrelid
      JOIN unnest(constraint_metadata.conkey) WITH ORDINALITY
        AS constraint_columns(attnum, ordinality) ON TRUE
      JOIN pg_attribute attribute_metadata
        ON attribute_metadata.attrelid = table_metadata.oid
        AND attribute_metadata.attnum = constraint_columns.attnum
      WHERE table_metadata.relname = 'group'
        AND constraint_metadata.conname = 'group_id_organization_id_key'
        AND constraint_metadata.contype = 'u'
      """.trimIndent(),
    ) as String?
}
