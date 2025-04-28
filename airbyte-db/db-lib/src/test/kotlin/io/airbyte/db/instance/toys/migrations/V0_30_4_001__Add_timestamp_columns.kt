/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys.migrations

import io.airbyte.db.instance.toys.ToysDatabaseConstants
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

@Suppress("ktlint:standard:class-naming")
class V0_30_4_001__Add_timestamp_columns : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val dsl = DSL.using(context.connection)
    dsl
      .alterTable(ToysDatabaseConstants.TABLE_NAME)
      .addColumn(DSL.field("created_at", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()).nullable(false)))
      .execute()
    dsl
      .alterTable(ToysDatabaseConstants.TABLE_NAME)
      .addColumn(DSL.field("updated_at", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()).nullable(false)))
      .execute()
  }
}
