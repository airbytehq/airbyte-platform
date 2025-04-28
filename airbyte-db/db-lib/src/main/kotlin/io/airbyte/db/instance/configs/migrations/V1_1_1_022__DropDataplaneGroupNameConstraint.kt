/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drops the dataplane_group_name_matches_geography constraint from the dataplane_group table.
 */

@Suppress("ktlint:standard:class-naming")
class V1_1_1_022__DropDataplaneGroupNameConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val connection = context.connection
    val ctx = DSL.using(connection)

    val constraintExists =
      ctx.fetchExists(
        DSL
          .selectOne()
          .from("information_schema.table_constraints")
          .where(DSL.field("table_name").eq("dataplane_group"))
          .and(DSL.field("constraint_name").eq("dataplane_group_name_matches_geography")),
      )

    if (constraintExists) {
      ctx
        .alterTable("dataplane_group")
        .dropConstraint("dataplane_group_name_matches_geography")
        .execute()
      log.info { "Dropped constraint: dataplane_group_name_matches_geography" }
    } else {
      log.info { "Constraint 'dataplane_group_name_matches_geography' does not exist. Nothing to drop." }
    }
  }
}
