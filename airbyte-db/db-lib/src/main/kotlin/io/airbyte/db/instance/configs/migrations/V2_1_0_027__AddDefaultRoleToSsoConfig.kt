/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.SSO_CONFIG_TABLE
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

/**
 * Add default_role column to sso_config table to support configurable JIT-provisioned organization roles.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_027__AddDefaultRoleToSsoConfig : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val ctx = DSL.using(context.connection)

    addDefaultRoleColumnToSsoConfig(ctx)
  }

  companion object {
    fun addDefaultRoleColumnToSsoConfig(ctx: DSLContext) {
      // permission_type is an existing Postgres enum shared by permission-bearing config tables.
      ctx.execute("ALTER TABLE $SSO_CONFIG_TABLE ADD COLUMN default_role permission_type")
    }
  }
}
