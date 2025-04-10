/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

@Suppress("ktlint:standard:class-naming")
class V0_57_2_001__AddRefreshJobType : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val ctx = DSL.using(context.connection)
    ctx.alterType("job_config_type").addValue("refresh").execute()
  }
}
