/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory.create
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.configs.migrations.V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.Companion.backfillSupportLevel
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersionTest : AbstractConfigsDatabaseTest() {
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

    val previousMigration: BaseJavaMigration = V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()
  }

  @Test
  @Throws(Exception::class)
  fun testBackfillSupportLevel() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    val numberOfAdvs = 10

    insertAdvWithReleaseStageAndSupportLevel(
      ctx,
      V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
      V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.certified,
    )

    for (i in 0..<numberOfAdvs) {
      insertAdvWithReleaseStageAndSupportLevel(
        ctx,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
      )
      insertAdvWithReleaseStageAndSupportLevel(
        ctx,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.beta,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
      )
      insertAdvWithReleaseStageAndSupportLevel(
        ctx,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.generally_available,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
      )
      insertAdvWithReleaseStageAndSupportLevel(
        ctx,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.custom,
        V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
      )
    }

    // assert that all advs have support level "none"
    val preAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL.field("support_level").eq(
            V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
          ),
        ).fetch()

    Assertions.assertEquals(numberOfAdvs * 4, preAdvs.size)

    backfillSupportLevel(ctx)

    // assert that all alpha advs have support level set to community
    val alphaAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL
            .field("release_stage")
            .eq(
              V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
            ).and(
              DSL.field("support_level").eq(
                V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.community,
              ),
            ),
        ).fetch()
    Assertions.assertEquals(numberOfAdvs, alphaAdvs.size)

    // assert that all beta advs have support level set to community
    val betaAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL
            .field("release_stage")
            .eq(
              V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.beta,
            ).and(
              DSL.field("support_level").eq(
                V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.community,
              ),
            ),
        ).fetch()

    Assertions.assertEquals(numberOfAdvs, betaAdvs.size)

    // assert that all generally_available advs have support level set to certified
    val gaAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL
            .field("release_stage")
            .eq(
              V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.generally_available,
            ).and(
              DSL.field("support_level").eq(
                V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.certified,
              ),
            ),
        ).fetch()

    Assertions.assertEquals(numberOfAdvs, gaAdvs.size)

    // assert that all custom advs have support level set to none
    val customAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL
            .field("release_stage")
            .eq(
              V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.custom,
            ).and(
              DSL.field("support_level").eq(
                V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.none,
              ),
            ),
        ).fetch()

    Assertions.assertEquals(numberOfAdvs, customAdvs.size)

    // assert that there is one adv with support level certified and release stage alpha (i.e. did not
    // get overwritten)
    val certifiedAdvs: List<Record> =
      ctx
        .select()
        .from(DSL.table("actor_definition_version"))
        .where(
          DSL
            .field("release_stage")
            .eq(
              V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
            ).and(
              DSL.field("support_level").eq(
                V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel.certified,
              ),
            ),
        ).fetch()

    Assertions.assertEquals(1, certifiedAdvs.size)
  }

  @Test
  fun testNoDefaultSupportLevel() {
    val ctx = getDslContext()

    // ignore all foreign key constraints
    ctx.execute("SET session_replication_role = replica;")

    backfillSupportLevel(ctx)

    Assertions.assertThrows(
      RuntimeException::class.java,
    ) {
      insertWithoutSupportLevel(
        ctx,
      )
    }
  }

  companion object {
    private fun insertAdvWithReleaseStageAndSupportLevel(
      ctx: DSLContext,
      releaseStage: V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage,
      supportLevel: V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.SupportLevel,
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
          DSL.field("release_stage"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          supportLevel,
          releaseStage,
        ).execute()
    }

    private fun insertWithoutSupportLevel(ctx: DSLContext) {
      ctx
        .insertInto(DSL.table("actor_definition_version"))
        .columns(
          DSL.field("id"),
          DSL.field("actor_definition_id"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("spec"),
          DSL.field("release_stage"),
        ).values(
          UUID.randomUUID(),
          UUID.randomUUID(),
          "repo",
          "1.0.0",
          JSONB.valueOf("{}"),
          V0_50_23_004__NaivelyBackfillSupportLevelForActorDefitionVersion.ReleaseStage.alpha,
        ).execute()
    }
  }
}
