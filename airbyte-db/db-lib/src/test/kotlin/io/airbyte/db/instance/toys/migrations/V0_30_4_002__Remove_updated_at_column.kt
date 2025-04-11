/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys.migrations

import io.airbyte.db.instance.toys.ToysDatabaseConstants
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

@Suppress("ktlint:standard:class-naming")
class V0_30_4_002__Remove_updated_at_column : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val dsl = DSL.using(context.connection)
    dsl
      .alterTable(ToysDatabaseConstants.TABLE_NAME)
      .dropColumn(DSL.field("updated_at"))
      .execute()
  }
}
