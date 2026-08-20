/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Creates the table holding Data Worker capacity allocated to an organization per region.
 *
 * Committed Data Worker capacity is sold at the organization level but is meant to be enforced per
 * dataplane group, which a single Stigg entitlement value cannot express. This table stores the
 * region -> allocated capacity pairs that enforcement will read instead.
 *
 * Capacity is REAL rather than an integer so fractional Data Workers can be sold later.
 *
 * The schema is reversible with a follow-up migration that drops the table. Flyway migrations are
 * not rolled back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_038__CreateDataWorkerAllocatedCapacityTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createDataWorkerAllocatedCapacityTable(ctx)
  }

  companion object {
    @JvmStatic
    fun createDataWorkerAllocatedCapacityTable(ctx: DSLContext) {
      createTable(ctx)
      createIndexes(ctx)
    }

    private fun createTable(ctx: DSLContext) {
      ctx.execute(
        """
        CREATE TABLE data_worker_allocated_capacity(
          id UUID PRIMARY KEY,
          organization_id UUID NOT NULL,
          dataplane_group_id UUID NOT NULL,
          allocated_capacity REAL NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          CONSTRAINT data_worker_allocated_capacity_non_negative_check CHECK (allocated_capacity >= 0)
        )
        """.trimIndent(),
      )
    }

    private fun createIndexes(ctx: DSLContext) {
      // One allocation row per (organization, region). Also serves organization_id-prefixed lookups,
      // which is how enforcement and the allocation API read this table.
      ctx.execute(
        """
        CREATE UNIQUE INDEX data_worker_allocated_capacity_org_dataplane_group_idx
        ON data_worker_allocated_capacity(organization_id, dataplane_group_id)
        """.trimIndent(),
      )
    }
  }
}
