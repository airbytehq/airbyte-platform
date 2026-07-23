/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_023__RelaxPrivateLinkNameUniquenessForTombstones : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    dropOldUniqueConstraint(ctx)
    createPartialUniqueIndex(ctx)

    log.info { "Migration completed: ${javaClass.simpleName}" }
  }

  companion object {
    private const val TABLE_NAME = "private_link"
    private const val INDEX_NAME = "private_link_workspace_id_name_key"

    fun dropOldUniqueConstraint(ctx: DSLContext) {
      ctx
        .alterTable(TABLE_NAME)
        .dropConstraintIfExists(INDEX_NAME)
        .execute()
    }

    fun createPartialUniqueIndex(ctx: DSLContext) {
      // Status is a Postgres enum; cast the literal so the predicate is stored
      // by Postgres in a form that survives pg_dump round-trips.
      ctx.execute(
        "CREATE UNIQUE INDEX $INDEX_NAME ON $TABLE_NAME (workspace_id, name) " +
          "WHERE status <> 'deleted'::private_link_status",
      )
    }
  }
}
