/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_23_003__AddSupportLevelToActorDefinitionVersion.Companion.addSupportLevelToActorDefinitionVersion
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_23_003__AddSupportLevelToActorDefinitionVersionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_23_003__AddSupportLevelToActorDefinitionVersionTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V0_50_6_002__AddDefaultVersionIdToActor()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = getDslContext()

    // ignore all foreign key constraints
    context.execute("SET session_replication_role = replica;")

    Assertions.assertFalse(supportLevelColumnExists(context))

    addSupportLevelToActorDefinitionVersion(context)

    Assertions.assertTrue(supportLevelColumnExists(context))

    assertSupportLevelEnumWorks(context)
  }

  companion object {
    private fun supportLevelColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition_version")
              .and(DSL.field("column_name").eq("support_level")),
          ),
      )

    private fun assertSupportLevelEnumWorks(ctx: DSLContext) {
      Assertions.assertDoesNotThrow {
        insertWithSupportLevel(
          ctx,
          V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel.community,
        )
        insertWithSupportLevel(
          ctx,
          V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel.certified,
        )
        insertWithSupportLevel(
          ctx,
          V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel.none,
        )
      }

      Assertions.assertThrows(
        Exception::class.java,
      ) {
        insertWithSupportLevel(
          ctx,
          V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel.valueOf(
            "invalid",
          ),
        )
      }
    }

    private fun insertWithSupportLevel(
      ctx: DSLContext,
      supportLevel: V0_50_23_003__AddSupportLevelToActorDefinitionVersion.SupportLevel,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("support_level"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          supportLevel,
        ).execute()
    }
  }
}
