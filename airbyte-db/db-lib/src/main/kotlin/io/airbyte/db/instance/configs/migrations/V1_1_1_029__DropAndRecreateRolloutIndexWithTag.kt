/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_029__DropAndRecreateRolloutIndexWithTag : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    doMigration(DSL.using(context.connection))
  }

  companion object {
    private const val TABLE = "connector_rollout"
    private const val OLD_INDEX = "actor_definition_id_state_unique_idx"
    private const val NEW_INDEX = "actor_definition_id_state_tag_unique_idx"

    @JvmStatic
    @VisibleForTesting
    fun doMigration(ctx: DSLContext) {
      addTagField(ctx)
      dropOldIndex(ctx)
      createNewIndex(ctx)
    }

    @JvmStatic
    fun addTagField(ctx: DSLContext) {
      ctx
        .alterTable(TABLE)
        .addColumn("tag", SQLDataType.VARCHAR(256).nullable(true))
        .execute()
    }

    @JvmStatic
    fun dropOldIndex(ctx: DSLContext) {
      ctx
        .dropIndexIfExists(OLD_INDEX)
        .on(TABLE)
        .execute()
    }

    @JvmStatic
    fun createNewIndex(ctx: DSLContext) {
      ctx
        .createUniqueIndex(NEW_INDEX)
        .on(DSL.table(TABLE), DSL.field("actor_definition_id"), DSL.field("tag"))
        .where(
          DSL.field("state").`in`(
            "initialized",
            "in_progress",
            "paused",
            "workflow_started",
            "finalizing",
            "errored",
          ),
        ).execute()
    }
  }
}
