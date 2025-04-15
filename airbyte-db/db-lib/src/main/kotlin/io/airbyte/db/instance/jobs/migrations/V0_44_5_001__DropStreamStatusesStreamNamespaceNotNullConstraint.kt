/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drops not null constraint on stream_statuses#stream_namespace column.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_5_001__DropStreamStatusesStreamNamespaceNotNullConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx
      .alterTable("stream_statuses")
      .alter("stream_namespace")
      .dropNotNull()
      .execute()

    log.info { "Completed migration: ${javaClass.simpleName}" }
  }
}
