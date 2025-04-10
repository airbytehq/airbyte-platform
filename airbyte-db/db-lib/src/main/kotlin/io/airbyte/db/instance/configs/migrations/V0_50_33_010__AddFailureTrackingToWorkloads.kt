/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_33_010__AddFailureTrackingToWorkloads : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    ctx
      .alterTable(WORKLOAD_TABLE)
      .addColumnIfNotExists(DSL.field("termination_source", SQLDataType.VARCHAR.nullable(true)))
      .execute()
    ctx
      .alterTable(WORKLOAD_TABLE)
      .addColumnIfNotExists(DSL.field("termination_reason", SQLDataType.CLOB.nullable(true)))
      .execute()
  }

  companion object {
    private const val WORKLOAD_TABLE = "workload"
  }
}
