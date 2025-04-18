/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V1_1_1_002__AddEnterpriseToActorDefinition.Companion.addEnterpriseColumn
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V1_1_1_002_AddEnterpriseToActorDefinitionTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V1_1_1_002_AddEnterpriseToActorDefinitionTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database, flyway)

    val previousMigration: BaseJavaMigration = V1_1_1_001__AddResourceRequirementsToActor()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = getDslContext()

    Assertions.assertFalse(enterpriseColumnExists(context))

    val id = UUID.randomUUID()
    context
      .insertInto(DSL.table("actor_definition"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("actor_type"),
      ).values(
        id,
        "name",
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
      ).execute()

    addEnterpriseColumn(context)

    Assertions.assertTrue(enterpriseColumnExists(context))
    Assertions.assertTrue(enterpriseDefaultsToFalse(context, id))
  }

  companion object {
    protected fun enterpriseColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq("enterprise")),
          ),
      )

    protected fun enterpriseDefaultsToFalse(
      ctx: DSLContext,
      id: UUID?,
    ): Boolean {
      val record =
        ctx.fetchOne(
          DSL
            .select()
            .from("actor_definition")
            .where(DSL.field("id").eq(id)),
        )

      return record!!["enterprise"] == false
    }
  }
}
