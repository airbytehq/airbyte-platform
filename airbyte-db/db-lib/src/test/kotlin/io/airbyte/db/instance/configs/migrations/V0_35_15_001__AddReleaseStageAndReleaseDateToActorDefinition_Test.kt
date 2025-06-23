/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.Companion.addReleaseDateColumn
import io.airbyte.db.instance.configs.migrations.V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.Companion.addReleaseStageColumn
import io.airbyte.db.instance.configs.migrations.V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.Companion.createReleaseStageEnum
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition_Test : AbstractConfigsDatabaseTest() {
  @Test
  fun test() {
    val context = dslContext!!

    // necessary to add actor_definition table
    V0_32_8_001__AirbyteConfigDatabaseDenormalization.migrate(context)

    Assertions.assertFalse(releaseStageColumnExists(context))
    Assertions.assertFalse(releaseDateColumnExists(context))

    createReleaseStageEnum(context)
    addReleaseStageColumn(context)
    addReleaseDateColumn(context)

    Assertions.assertTrue(releaseStageColumnExists(context))
    Assertions.assertTrue(releaseDateColumnExists(context))

    assertReleaseStageEnumWorks(context)
  }

  companion object {
    private fun releaseStageColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq("release_stage")),
          ),
      )

    private fun releaseDateColumnExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.columns")
          .where(
            DSL
              .field("table_name")
              .eq("actor_definition")
              .and(DSL.field("column_name").eq("release_date")),
          ),
      )

    private fun assertReleaseStageEnumWorks(ctx: DSLContext) {
      Assertions.assertDoesNotThrow {
        insertWithReleaseStage(
          ctx,
          V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage.alpha,
        )
        insertWithReleaseStage(
          ctx,
          V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage.beta,
        )
        insertWithReleaseStage(
          ctx,
          V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage.generally_available,
        )
        insertWithReleaseStage(
          ctx,
          V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage.custom,
        )
      }
    }

    private fun insertWithReleaseStage(
      ctx: DSLContext,
      releaseStage: V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage,
    ) {
      ctx
        .insertInto(DSL.table("actor_definition"))
        .columns(
          DSL.field("id"),
          DSL.field("name"),
          DSL.field("docker_repository"),
          DSL.field("docker_image_tag"),
          DSL.field("actor_type"),
          DSL.field("spec"),
          DSL.field("release_stage"),
        ).values(
          UUID.randomUUID(),
          "name",
          "repo",
          "1.0.0",
          V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source,
          JSONB.valueOf("{}"),
          releaseStage,
        ).execute()
    }
  }
}
