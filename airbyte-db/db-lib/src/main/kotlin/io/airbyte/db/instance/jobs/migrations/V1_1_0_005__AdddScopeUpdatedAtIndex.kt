/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_0_005__AdddScopeUpdatedAtIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)
    ctx
      .query("CREATE INDEX IF NOT EXISTS $JOBS_SCOPE_UPDATED_AT_INDEX ON jobs(scope, updated_at DESC)")
      .execute()
  }

  companion object {
    private const val JOBS_SCOPE_UPDATED_AT_INDEX: String = "scope_updated_at_idx"
  }
}
