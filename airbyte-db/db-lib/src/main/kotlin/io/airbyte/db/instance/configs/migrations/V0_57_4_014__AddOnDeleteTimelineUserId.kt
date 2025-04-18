/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_4_014__AddOnDeleteTimelineUserId : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx.alterTable(CONNECTION_TIMELINE_EVENT).dropConstraint("connection_timeline_event_user_id_fkey").execute()
    ctx
      .alterTable("connection_timeline_event")
      .add(
        DSL
          .foreignKey("user_id")
          .references("user", "id")
          .onDeleteSetNull(),
      ).execute()
  }

  companion object {
    private val CONNECTION_TIMELINE_EVENT = DSL.table("connection_timeline_event")
  }
}
