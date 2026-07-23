/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.GROUP_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Adds the composite Group key required by organization-scoped foreign keys.
 *
 * Group IDs remain globally unique through the primary key. This additional key allows later
 * migrations to enforce Group and organization consistency with a composite foreign key.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_034__AddGroupOrganizationKey : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addGroupOrganizationKey(ctx)
  }

  companion object {
    @JvmStatic
    fun addGroupOrganizationKey(ctx: DSLContext) {
      ctx.execute(
        """
        ALTER TABLE "$GROUP_TABLE"
          ADD CONSTRAINT group_id_organization_id_key UNIQUE (id, organization_id)
        """.trimIndent(),
      )
    }
  }
}
