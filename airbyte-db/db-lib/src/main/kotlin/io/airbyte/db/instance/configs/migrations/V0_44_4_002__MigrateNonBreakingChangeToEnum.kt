/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.StandardSync
import io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE
import io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Record1
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.util.UUID

/**
 * This migration is the first step to get rid of the non_breaking_schema_change_preference in the
 * connection table. The issue with this column is that it got create as a varchar instead of an
 * enum, this makes it hard to add value. This migration deletes the auto_propagation_status column
 * in the schema_management table as well as the type of it, then recreate the enum with the right
 * value and re-add the column. Finally, it creates entries in the schema_management table for all
 * the connections with a disable preference for the non_breaking_schema_change.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_4_002__MigrateNonBreakingChangeToEnum : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val ctx = DSL.using(context.connection)

    dropExistingColumnAndType(ctx)
    recreateEnumType(ctx)
    addColumnWithUpdateType(ctx)
    migrateExistingDisableConnection(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class AutoPropagationStatus(
    private val literal: String,
  ) : EnumType {
    disabled(StandardSync.NonBreakingChangesPreference.DISABLE.value()),
    ignore(StandardSync.NonBreakingChangesPreference.IGNORE.value()),
    propagate_column(StandardSync.NonBreakingChangesPreference.PROPAGATE_COLUMNS.value()),
    propagate_fully(StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY.value()),
    ;

    override fun getCatalog(): Catalog? = if (schema == null) null else schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "auto_propagation_status"

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val AUTO_PROPAGATION_STATUS = "auto_propagation_status"

    private val OLD_AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field(
        AUTO_PROPAGATION_STATUS,
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_44_3_001__AddSchemaManagementConfigurationExternalTable.AutoPropagationStatus::class.java,
          ).nullable(false),
      )

    private val NEW_AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field(
        AUTO_PROPAGATION_STATUS,
        SQLDataType.VARCHAR
          .asEnumDataType(
            AutoPropagationStatus::class.java,
          ).nullable(false)
          .defaultValue(AutoPropagationStatus.ignore),
      )

    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID)
    private val CONNECTION_ID_COLUMN = DSL.field("connection_id", SQLDataType.UUID)
    private val CREATED_AT_COLUMN = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)
    private val UPDATE_AT_COLUMN = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE)

    /**
     * Drop the column in the schema_management table since the 2 enum values are not being used. This
     * will be recreated later, using the values from the connection table
     */
    private fun dropExistingColumnAndType(ctx: DSLContext) {
      ctx.alterTable(SCHEMA_MANAGEMENT_TABLE).dropColumn(OLD_AUTO_PROPAGATION_STATUS_COLUMN).execute()
      ctx.dropTypeIfExists(AUTO_PROPAGATION_STATUS).execute()
    }

    /**
     * Recreate the enum type using the config Enum values.
     */
    private fun recreateEnumType(ctx: DSLContext) {
      ctx
        .createType(AUTO_PROPAGATION_STATUS)
        .asEnum(
          StandardSync.NonBreakingChangesPreference.DISABLE.value(),
          StandardSync.NonBreakingChangesPreference.IGNORE.value(),
          StandardSync.NonBreakingChangesPreference.PROPAGATE_COLUMNS.value(),
          StandardSync.NonBreakingChangesPreference.PROPAGATE_FULLY.value(),
        ).execute()
    }

    /**
     * Add the column to the schema management table.
     */
    private fun addColumnWithUpdateType(ctx: DSLContext) {
      ctx.alterTable(SCHEMA_MANAGEMENT_TABLE).addColumn(NEW_AUTO_PROPAGATION_STATUS_COLUMN).execute()
    }

    /**
     * Migrate the connection that are currently disable. At the time this PR is being created, there is
     * 29 connections with the non_breaking_change_preference set to disable.
     */
    private fun migrateExistingDisableConnection(ctx: DSLContext) {
      val disabledConnectionIds: List<UUID> =
        ctx
          .select(ID_COLUMN)
          .from(CONNECTION_TABLE)
          .where(
            DSL
              .field("non_breaking_change_preference")
              .eq(
                DSL.cast(
                  "disable",
                  SQLDataType.VARCHAR.asEnumDataType(
                    V0_40_11_002__AddSchemaChangeColumnsToConnections.NonBreakingChangePreferenceType::class.java,
                  ),
                ),
              ),
          ).stream()
          .map { record: Record1<UUID> -> record.getValue(ID_COLUMN) }
          .toList()

      disabledConnectionIds.forEach { connectionId: UUID ->
        ctx
          .insertInto(DSL.table(SCHEMA_MANAGEMENT_TABLE))
          .set(ID_COLUMN, UUID.randomUUID())
          .set(CONNECTION_ID_COLUMN, connectionId)
          .set(NEW_AUTO_PROPAGATION_STATUS_COLUMN, AutoPropagationStatus.disabled)
          .set(CREATED_AT_COLUMN, DSL.currentOffsetDateTime())
          .set(UPDATE_AT_COLUMN, DSL.currentOffsetDateTime())
          .execute()
      }
    }
  }
}
