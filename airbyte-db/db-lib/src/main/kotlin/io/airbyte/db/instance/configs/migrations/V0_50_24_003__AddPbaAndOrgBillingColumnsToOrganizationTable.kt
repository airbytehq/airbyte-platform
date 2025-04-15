/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger { }

@Suppress("ktlint:standard:class-naming")
class V0_50_24_003__AddPbaAndOrgBillingColumnsToOrganizationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    // Add pba column to organization table. This boolean flag will let us know if an organization is a
    // PbA (Powered by Airbyte) customer or not.
    val pba = DSL.field(PBA_COLUMN, SQLDataType.BOOLEAN.defaultValue(false).nullable(false))
    ctx
      .alterTable(ORGANIZATION_TABLE)
      .addColumnIfNotExists(pba)
      .execute()

    // Add org_level_billing column to organization table. This boolean flag will let us know if
    // workspaces in this organization should be billed at the organization level or at the workspace level.
    val orgLevelBilling = DSL.field(ORG_LEVEL_BILLING_COLUMN, SQLDataType.BOOLEAN.defaultValue(false).nullable(false))
    ctx
      .alterTable(ORGANIZATION_TABLE)
      .addColumnIfNotExists(orgLevelBilling)
      .execute()

    log.info { "Migration finished!" }
  }

  companion object {
    private const val ORGANIZATION_TABLE = "organization"
    private const val PBA_COLUMN = "pba"
    private const val ORG_LEVEL_BILLING_COLUMN = "org_level_billing"
  }
}
