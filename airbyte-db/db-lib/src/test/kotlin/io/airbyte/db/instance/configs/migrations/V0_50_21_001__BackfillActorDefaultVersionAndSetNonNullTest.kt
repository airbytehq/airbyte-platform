/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull.Companion.backfillActorDefaultVersionId
import io.airbyte.db.instance.configs.migrations.V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull.Companion.setNonNull
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_21_001__BackfillActorDefaultVersionAndSetNonNullTest : AbstractConfigsDatabaseTest() {
  @BeforeEach
  fun beforeEach() {
    val flyway =
      create(
        dataSource,
        "V0_50_21_001__BackfillActorDefaultVersionAndSetNonNullTest.java",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )
    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)

    val previousMigration: BaseJavaMigration = V0_50_20_001__MakeManualNullableForRemoval()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  private fun getDefaultVersionIdForActorId(
    ctx: DSLContext,
    actorId: UUID,
  ): UUID? {
    val actor =
      ctx
        .select(DEFAULT_VERSION_ID_COL)
        .from(ACTOR)
        .where(ID_COL.eq(actorId))
        .fetchOne()

    if (actor == null) {
      return null
    }

    return actor.get(DEFAULT_VERSION_ID_COL)
  }

  @Test
  fun testBackFillActorDefaultVersionId() {
    val ctx = dslContext!!
    insertDependencies(ctx)

    ctx
      .insertInto(ACTOR)
      .columns(
        ID_COL,
        ACTOR_DEFINITION_ID_COL,
        DSL.field("workspace_id"),
        DSL.field("name"),
        DSL.field("configuration"),
        DSL.field("actor_type"),
      ).values(
        ACTOR_ID,
        ACTOR_DEFINITION_ID,
        WORKSPACE_ID,
        "My Source",
        JSONB.valueOf("{}"),
        V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
      ).execute()

    Assertions.assertNull(getDefaultVersionIdForActorId(ctx, ACTOR_ID))

    backfillActorDefaultVersionId(ctx)

    Assertions.assertEquals(VERSION_ID, getDefaultVersionIdForActorId(ctx, ACTOR_ID))
  }

  @Test
  fun testActorDefaultVersionIdIsNotNull() {
    val context = dslContext!!

    setNonNull(context)

    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(ACTOR)
          .columns(
            ID_COL,
            ACTOR_DEFINITION_ID_COL,
            DSL.field("workspace_id"),
            DSL.field("name"),
            DSL.field("configuration"),
            DSL.field("actor_type"),
          ).values(
            UUID.randomUUID(),
            UUID.randomUUID(),
            UUID.randomUUID(),
            "My Source",
            JSONB.valueOf("{}"),
            V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("null value in column \"default_version_id\" of relation \"actor\" violates not-null constraint"))
  }

  companion object {
    private val ACTOR = DSL.table("actor")
    private val ACTOR_DEFINITION = DSL.table("actor_definition")
    private val ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version")
    private val WORKSPACE = DSL.table("workspace")

    private val ID_COL = DSL.field("id", SQLDataType.UUID)
    private val DEFAULT_VERSION_ID_COL = DSL.field("default_version_id", SQLDataType.UUID)
    private val ACTOR_DEFINITION_ID_COL = DSL.field("actor_definition_id", SQLDataType.UUID)

    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val ACTOR_ID: UUID = UUID.randomUUID()
    private val VERSION_ID: UUID = UUID.randomUUID()

    fun insertDependencies(ctx: DSLContext) {
      ctx
        .insertInto(WORKSPACE)
        .columns(
          ID_COL,
          DSL.field("name"),
          DSL.field("slug"),
          DSL.field("initial_setup_complete"),
        ).values(
          WORKSPACE_ID,
          "name1",
          "default",
          true,
        ).execute()

      ctx
        .insertInto(ACTOR_DEFINITION)
        .columns(
          ID_COL,
          DSL.field("name"),
          DSL.field("actor_type"),
        ).values(
          ACTOR_DEFINITION_ID,
          "source def name",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
        ).execute()

      ctx
        .insertInto(ACTOR_DEFINITION_VERSION)
        .columns(
          ID_COL,
          ACTOR_DEFINITION_ID_COL,
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
        ).values(VERSION_ID, ACTOR_DEFINITION_ID, "airbyte/some-source", "1.0.0", JSONB.valueOf("{}"))
        .execute()

      ctx
        .update(ACTOR_DEFINITION)
        .set(DEFAULT_VERSION_ID_COL, VERSION_ID)
        .where(ID_COL.eq(ACTOR_DEFINITION_ID))
        .execute()
    }
  }
}
