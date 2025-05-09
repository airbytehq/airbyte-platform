/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_008__FixesForConnectionTemplates : BaseJavaMigration() {
  private val oldNonBreakingChangePreferenceType = "non_breaking_change_preference_type"
  private val newNonBreakingChangePreferenceType = "non_breaking_changes_preference_type"
  private val oldNonBreakingChangePreferenceField = "non_breaking_change_preference"
  private val newNonBreakingChangePreferenceField = "non_breaking_changes_preference"

  private val connectionTemplateTable = "connection_template"

  private val dataplaneGroupIdColumn = "dataplace_group_id" // There is a typo in the column name in the database

  private val defaultGeographyField = "default_geography"
  private val syncOnCreateField = "sync_on_create"

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    ctx.alterType(oldNonBreakingChangePreferenceType).addValue("propagate_columns").execute()
    ctx.alterType(oldNonBreakingChangePreferenceType).addValue("propagate_fully").execute()
    ctx.alterType(oldNonBreakingChangePreferenceType).renameTo(newNonBreakingChangePreferenceType)

    ctx
      .alterTable(connectionTemplateTable)
      .renameColumn(oldNonBreakingChangePreferenceField)
      .to(newNonBreakingChangePreferenceField)
      .execute()

    ctx
      .alterTable(connectionTemplateTable)
      .drop(dataplaneGroupIdColumn)
      .execute()

    ctx
      .alterTable(connectionTemplateTable)
      .add(defaultGeographyField, SQLDataType.VARCHAR)
      .execute()

    ctx
      .alterTable(connectionTemplateTable)
      .add(syncOnCreateField, SQLDataType.BOOLEAN.notNull().defaultValue(true))
      .execute()
  }
}
