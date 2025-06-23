/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_5_004__AddActorDefinitionBreakingChangeTable.Companion.createBreakingChangesTable
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.time.LocalDate
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_5_004__AddActorDefinitionBreakingChangeTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_5_004__AddActorDefinitionBreakingChangeTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_4_002__DropActorDefinitionVersionedCols()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!
    createBreakingChangesTable(context)

    val actorDefinitionId = UUID.randomUUID()

    context
      .insertInto(DSL.table("actor_definition"))
      .columns(
        DSL.field("id"),
        DSL.field("name"),
        DSL.field("actor_type"),
      ).values(
        actorDefinitionId,
        "name",
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
      ).execute()

    // assert can insert
    Assertions.assertDoesNotThrow {
      context
        .insertInto(
          DSL.table(
            ACTOR_DEFINITION_BREAKING_CHANGE,
          ),
        ).columns(
          DSL.field(ACTOR_DEFINITION_ID),
          DSL.field(VERSION),
          DSL.field(MIGRATION_DOCUMENTATION_URL),
          DSL.field(UPGRADE_DEADLINE),
          DSL.field(MESSAGE),
        ).values(
          actorDefinitionId,
          "1.0.0",
          "https://docs.airbyte.com/migration",
          LocalDate.of(2025, 1, 1),
          "some reason",
        ).execute()
    }

    // assert uniqueness for actor def + version
    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(
            DSL.table(
              ACTOR_DEFINITION_BREAKING_CHANGE,
            ),
          ).columns(
            DSL.field(ACTOR_DEFINITION_ID),
            DSL.field(VERSION),
            DSL.field(MIGRATION_DOCUMENTATION_URL),
            DSL.field(UPGRADE_DEADLINE),
            DSL.field(MESSAGE),
          ).values(
            actorDefinitionId,
            "1.0.0",
            "https://docs.airbyte.com/migration/2",
            LocalDate.of(2024, 1, 1),
            "some other reason",
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("duplicate key value violates unique constraint"))
  }

  companion object {
    private const val ACTOR_DEFINITION_BREAKING_CHANGE = "actor_definition_breaking_change"
    private const val ACTOR_DEFINITION_ID = "actor_definition_id"
    private const val VERSION = "version"
    private const val MIGRATION_DOCUMENTATION_URL = "migration_documentation_url"
    private const val UPGRADE_DEADLINE = "upgrade_deadline"
    private const val MESSAGE = "message"
  }
}
