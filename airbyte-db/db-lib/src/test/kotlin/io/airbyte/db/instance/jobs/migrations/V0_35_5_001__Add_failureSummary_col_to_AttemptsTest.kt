/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.db.instance.jobs.AbstractJobsDatabaseTest
import io.airbyte.db.instance.jobs.migrations.V0_35_5_001__Add_failureSummary_col_to_Attempts.Companion.addFailureSummaryColumn
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
internal class V0_35_5_001__Add_failureSummary_col_to_AttemptsTest : AbstractJobsDatabaseTest() {
  @Test
  fun test() {
    val context = dslContext!!
    Assertions.assertFalse(failureSummaryColumnExists(context))
    addFailureSummaryColumn(context)
    Assertions.assertTrue(failureSummaryColumnExists(context))
  }
}

private fun failureSummaryColumnExists(ctx: DSLContext): Boolean =
  ctx.fetchExists(
    DSL
      .select()
      .from("information_schema.columns")
      .where(
        DSL
          .field("table_name")
          .eq("attempts")
          .and(DSL.field("column_name").eq("failure_summary")),
      ),
  )
