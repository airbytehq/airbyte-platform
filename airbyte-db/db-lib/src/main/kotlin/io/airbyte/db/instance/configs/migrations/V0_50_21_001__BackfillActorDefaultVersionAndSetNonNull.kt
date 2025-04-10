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
 * Sets all actor's default_version_id to its actor_definition's default_version_id, and sets the
 * column to be non-null.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_21_001__BackfillActorDefaultVersionAndSetNonNull : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    backfillActorDefaultVersionId(ctx)
    setNonNull(ctx)
  }

  companion object {
    private val ACTOR_DEFINITION = DSL.table("actor_definition")
    private val ACTOR = DSL.table("actor")

    private val ID = DSL.field("id", SQLDataType.UUID)
    private val DEFAULT_VERSION_ID = DSL.field("default_version_id", SQLDataType.UUID)
    private val ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", SQLDataType.UUID)

    @JvmStatic
    fun backfillActorDefaultVersionId(ctx: DSLContext) {
      val actorDefinitions =
        ctx
          .select(ID, DEFAULT_VERSION_ID)
          .from(ACTOR_DEFINITION)
          .fetch()

      for (actorDefinition in actorDefinitions) {
        val actorDefinitionId = actorDefinition.get(ID)
        val defaultVersionId = actorDefinition.get(DEFAULT_VERSION_ID)

        ctx
          .update(ACTOR)
          .set(DEFAULT_VERSION_ID, defaultVersionId)
          .where(ACTOR_DEFINITION_ID.eq(actorDefinitionId))
          .execute()
      }
    }

    @JvmStatic
    fun setNonNull(ctx: DSLContext) {
      ctx
        .alterTable(ACTOR)
        .alterColumn(DEFAULT_VERSION_ID)
        .setNotNull()
        .execute()
    }
  }
}
