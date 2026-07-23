/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Adds a JSONB column to the workload table to support dual-write migration strategy.
 *
 * This migration is part of a gradual transition from the workload_label table to JSONB storage:
 * Phase 1: Write to both workload_label table AND workload.labels column (dual-write)
 * Phase 2: Feature flag to switch reads from workload_label to workload.labels
 * Phase 3: Eventually drop workload_label table after all workloads transition
 *
 * The JSONB column stores labels as key-value pairs: {"key1": "value1", "key2": "value2"}
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_006__AddLabelsJsonbColumnToWorkload : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    addLabelsColumn(ctx)
    log.info { "Migration finished!" }
  }

  companion object {
    private const val WORKLOAD_TABLE_NAME = "workload"
    private const val LABELS_COLUMN_NAME = "labels"

    fun addLabelsColumn(ctx: DSLContext) {
      ctx
        .alterTable(WORKLOAD_TABLE_NAME)
        .addColumnIfNotExists(
          DSL.field(LABELS_COLUMN_NAME, SQLDataType.JSONB.nullable(true)),
        ).execute()

      log.info { "Added labels JSONB column to workload table" }
    }
  }
}
