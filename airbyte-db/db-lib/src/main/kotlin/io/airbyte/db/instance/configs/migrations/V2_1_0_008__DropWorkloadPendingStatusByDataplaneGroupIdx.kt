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
class V2_1_0_008__DropWorkloadPendingStatusByDataplaneGroupIdx : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx
      .query(
        "DROP INDEX IF EXISTS workload_pending_status_by_dataplane_group_idx",
      ).execute()

    log.info { "Successfully dropped unused index workload_pending_status_by_dataplane_group_idx" }
  }
}
