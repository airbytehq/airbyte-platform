/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_006__DropRestrictiveRulesOnTemplateIds : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropNotNullOnTemplateId(ctx)
    dropCheckRuleOnTemplates(ctx)
  }

  companion object {
    private const val PARTIAL_USER_CONFIG_TABLE_NAME: String = "partial_user_config"
    private const val CONFIG_TEMPLATE_ID_COLUMN_NAME: String = "config_template_id"
    private const val CONFIG_TEMPLATE_TABLE_NAME: String = "config_template"
    private const val CONFIG_TEMPLATE_CHECK_RULE: String = "config_template_valid_reference_check"

    fun dropNotNullOnTemplateId(ctx: DSLContext) {
      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .alterColumn(CONFIG_TEMPLATE_ID_COLUMN_NAME)
        .dropNotNull()
        .execute()
    }

    fun dropCheckRuleOnTemplates(ctx: DSLContext) {
      ctx
        .alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .dropConstraint(CONFIG_TEMPLATE_CHECK_RULE)
        .execute()
    }
  }
}
