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
class V1_1_1_008__AddPendingStatusIndexToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx
      .query(
        "CREATE INDEX IF NOT EXISTS workload_pending_status_by_dataplane_group_idx ON workload(status, dataplane_group) where status = 'pending'",
      ).execute()
  }
}
