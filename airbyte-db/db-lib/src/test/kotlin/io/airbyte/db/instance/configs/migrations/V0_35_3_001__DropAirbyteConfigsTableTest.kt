/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_35_3_001__DropAirbyteConfigsTable.Companion.dropTable
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_35_3_001__DropAirbyteConfigsTableTest : AbstractConfigsDatabaseTest() {
  @Test
  fun test() {
    val context = dslContext!!
    Assertions.assertTrue(airbyteConfigsExists(context))
    dropTable(context)
    Assertions.assertFalse(airbyteConfigsExists(context))
  }

  companion object {
    private fun airbyteConfigsExists(ctx: DSLContext): Boolean =
      ctx.fetchExists(
        DSL
          .select()
          .from("information_schema.tables")
          .where(
            DSL
              .field("table_name")
              .eq("airbyte_configs")
              .and(DSL.field("table_schema").eq("public")),
          ),
      )
  }
}
