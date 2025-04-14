/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Adds the missing suggested_streams column to the actor_definition_version table. This migration
 * also fixes the type of the release_stage column to use the existing enum.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_5_002__AddSuggestedStreamsToActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addSuggestedStreams(ctx)
    fixReleaseStageType(ctx)
  }

  companion object {
    private fun fixReleaseStageType(ctx: DSLContext) {
      // Drops data, but is ok since the release_stage column had not been used yet.
      ctx
        .alterTable("actor_definition_version")
        .dropColumn("release_stage")
        .execute()

      ctx
        .alterTable("actor_definition_version")
        .addColumn(
          DSL.field(
            "release_stage",
            SQLDataType.VARCHAR
              .asEnumDataType(
                V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage::class.java,
              ).nullable(true),
          ),
        ).execute()
    }

    private fun addSuggestedStreams(ctx: DSLContext) {
      ctx
        .alterTable("actor_definition_version")
        .addColumnIfNotExists(DSL.field("suggested_streams", SQLDataType.JSONB.nullable(true)))
        .execute()
    }
  }
}
