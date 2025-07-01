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
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_41_006__AlterSupportLevelAddArchivedTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_41_006__AlterSupportLevelAddArchivedTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_41_006__AlterSupportLevelAddArchived()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(Exception::class)
  fun testArchivedConnectorVersion() {
    val ctx = dslContext!!

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    insertAdvWithSupportLevel(
      ctx,
      V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel.archived,
    ) // does not throw

    Assertions.assertThrows(
      IllegalArgumentException::class.java,
    ) {
      insertAdvWithSupportLevel(
        ctx,
        V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel.valueOf(
          "foo",
        ),
      )
    }
  }

  companion object {
    private fun insertAdvWithSupportLevel(
      ctx: DSLContext,
      supportLevel: V0_50_41_006__AlterSupportLevelAddArchived.SupportLevel,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("release_stage"),
          DSL.field("support_level"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
          supportLevel,
        ).execute()
    }
  }
}
