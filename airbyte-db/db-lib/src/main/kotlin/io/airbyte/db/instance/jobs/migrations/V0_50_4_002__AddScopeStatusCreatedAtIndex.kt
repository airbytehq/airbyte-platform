/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:filename")
/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_4_002__AddScopeStatusCreatedAtIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx
      .query("CREATE INDEX CONCURRENTLY IF NOT EXISTS $SCOPE_STATUS_CREATED_AT_INDEX_NAME ON jobs(scope, status, created_at DESC)")
      .execute()
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from within a transaction.
  override fun canExecuteInTransaction(): Boolean = false
}

const val SCOPE_STATUS_CREATED_AT_INDEX_NAME: String = "scope_status_created_at_idx"
