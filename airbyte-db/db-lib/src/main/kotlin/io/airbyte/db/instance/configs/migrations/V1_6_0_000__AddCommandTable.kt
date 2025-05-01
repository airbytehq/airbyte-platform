/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_000__AddCommandTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    // Table name
    private const val COMMANDS_TABLE: String = "commands"

    // Fields name
    private const val ID_FIELD: String = "id"
    private const val ORGANIZATION_ID_FIELD: String = "organization_id"
    private const val CREATED_AT_FIELD: String = "created_at"
    private const val UPDATED_AT_FIELD: String = "updated_at"
    private const val WORKLOAD_ID_FIELD: String = "workload_id"
    private const val COMMAND_TYPE_FIELD: String = "command_type" // Expected values: check, discover, replicate
    private const val COMMAND_INPUT_FIELD: String = "command_input"
    private const val WORKSPACE_ID_FIELD: String = "workspace_id"

    // Foreign key tables
    private const val WORKLOAD_TABLE = "workload"
    private const val WORKSPACE_TABLE = "workspace"
    private const val ORGANIZATION_TABLE = "organization"

    @VisibleForTesting
    fun doMigration(ctx: DSLContext) {
      // Call the function to create the command table
      createCommandTable(ctx)
      // Add other migration steps here if needed in the future
    }

    @VisibleForTesting
    fun createCommandTable(ctx: DSLContext) {
      // Create the command table
      ctx
        .createTableIfNotExists(COMMANDS_TABLE)
        // Define the 'id' column as VARCHAR, primary key, not null
        .column(ID_FIELD, SQLDataType.VARCHAR.notNull())
        // Define the 'workload_id' column as VARCHAR, not null
        .column(WORKLOAD_ID_FIELD, SQLDataType.VARCHAR.notNull())
        // Define the 'command_type' column as VARCHAR, not null
        // Consider adding a CHECK constraint later if needed: .constraint(DSL.check(DSL.field(COMMAND_TYPE_FIELD).`in`("check", "discover", "replicate")))
        .column(COMMAND_TYPE_FIELD, SQLDataType.VARCHAR.notNull())
        // Define the 'command_input' column as JSONB, not null
        .column(COMMAND_INPUT_FIELD, SQLDataType.JSONB.notNull())
        // Define the 'workspace_id' column as UUID, not null
        .column(WORKSPACE_ID_FIELD, SQLDataType.UUID.notNull())
        // Define the 'organization_id' column as UUID, not null
        .column(ORGANIZATION_ID_FIELD, SQLDataType.UUID.notNull())
        // Define the 'created_at' column with timestamp with timezone, not null, default to current time
        .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        // Define the 'updated_at' column with timestamp with timezone, not null, default to current time
        .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        // Set the primary key constraint on the 'id' field
        .primaryKey(ID_FIELD)
        // Execute the creation table statement
        .execute()

      // Foreign keys
      ctx.alterTable(COMMANDS_TABLE).add(DSL.foreignKey(WORKLOAD_ID_FIELD).references(WORKLOAD_TABLE).onDeleteCascade()).execute()
      ctx.alterTable(COMMANDS_TABLE).add(DSL.foreignKey(WORKSPACE_ID_FIELD).references(WORKSPACE_TABLE).onDeleteCascade()).execute()
      ctx.alterTable(COMMANDS_TABLE).add(DSL.foreignKey(ORGANIZATION_ID_FIELD).references(ORGANIZATION_TABLE).onDeleteCascade()).execute()
    }
  }
}
