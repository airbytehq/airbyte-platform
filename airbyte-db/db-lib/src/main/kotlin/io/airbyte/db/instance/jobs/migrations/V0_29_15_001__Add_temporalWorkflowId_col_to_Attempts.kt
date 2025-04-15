/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add temporal workflow id to attempt table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_29_15_001__Add_temporalWorkflowId_col_to_Attempts : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx
      .alterTable("attempts")
      .addColumnIfNotExists(DSL.field("temporal_workflow_id", SQLDataType.VARCHAR(256).nullable(true)))
      .execute()
  }
}
