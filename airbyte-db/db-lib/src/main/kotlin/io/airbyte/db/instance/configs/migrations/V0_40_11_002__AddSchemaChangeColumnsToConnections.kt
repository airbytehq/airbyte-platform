/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Add schema change column to connection migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_11_002__AddSchemaChangeColumnsToConnections : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    addNonBreakingChangePreferenceEnumTypes(ctx)

    addNotifySchemaChanges(ctx)
    addNonBreakingChangePreference(ctx)
    addBreakingChange(ctx)
  }

  internal enum class NonBreakingChangePreferenceType(
    private val literal: String,
  ) : EnumType {
    IGNORE("ignore"),
    DISABLE("disable"),
    ;

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "non_breaking_change_preference_type"
    }
  }

  companion object {
    private fun addNonBreakingChangePreferenceEnumTypes(ctx: DSLContext) {
      ctx
        .createType(NonBreakingChangePreferenceType.NAME)
        .asEnum(*NonBreakingChangePreferenceType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun addNotifySchemaChanges(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field("notify_schema_changes", SQLDataType.BOOLEAN.nullable(false).defaultValue(true)),
        ).execute()
    }

    private fun addNonBreakingChangePreference(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field(
            "non_breaking_change_preference",
            SQLDataType.VARCHAR
              .asEnumDataType(
                NonBreakingChangePreferenceType::class.java,
              ).nullable(false)
              .defaultValue(NonBreakingChangePreferenceType.IGNORE),
          ),
        ).execute()
    }

    private fun addBreakingChange(ctx: DSLContext) {
      ctx
        .alterTable("connection")
        .addColumnIfNotExists(
          DSL.field(
            "breaking_change",
            SQLDataType.BOOLEAN.nullable(false).defaultValue(false),
          ),
        ).execute()
    }
  }
}
