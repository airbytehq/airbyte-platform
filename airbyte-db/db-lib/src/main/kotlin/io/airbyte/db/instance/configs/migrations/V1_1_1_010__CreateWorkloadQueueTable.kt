/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_010__CreateWorkloadQueueTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    log.info { "Creating table" }
    createWorkloadQueueTable(ctx)

    log.info { "Creating indices" }
    createIndices(ctx)

    log.info { "Completed migration:${javaClass.simpleName}" }
  }

  companion object {
    private fun createWorkloadQueueTable(ctx: DSLContext) {
      // metadata
      val id = DSL.field("id", SQLDataType.UUID.notNull())

      // operational data
      val dataplaneGroup = DSL.field("dataplane_group", SQLDataType.VARCHAR(256).notNull())
      val priority = DSL.field("priority", SQLDataType.INTEGER.notNull())
      val workloadId = DSL.field("workload_id", SQLDataType.VARCHAR(256).notNull())
      val pollDeadline = DSL.field("poll_deadline", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
      val ackedAt = DSL.field("acked_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))

      // row timestamps
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("workload_queue")
        .columns(id, dataplaneGroup, priority, workloadId, pollDeadline, ackedAt, createdAt, updatedAt)
        .constraints(
          DSL.primaryKey(id),
          DSL.constraint("uniq_workload_id").unique("workload_id"),
        ).execute()
    }

    private fun createIndices(ctx: DSLContext) {
      ctx
        .query(
          (
            "CREATE INDEX IF NOT EXISTS dataplane_group_priority_poll_deadline_idx " +
              "ON workload_queue(dataplane_group, priority, poll_deadline) " +
              "WHERE acked_at IS NULL"
          ),
        ).execute()

      ctx.createIndexIfNotExists("workload_id_idx").on("workload_queue", "workload_id").execute()
    }
  }
}
