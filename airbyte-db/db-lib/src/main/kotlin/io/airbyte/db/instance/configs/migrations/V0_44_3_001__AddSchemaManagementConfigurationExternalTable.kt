/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE
import io.airbyte.db.instance.DatabaseConstants.SCHEMA_MANAGEMENT_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Migration to create a new table for schema management. For now, it only includes whether auto
 * propagation of schema changes is enabled. In the future, all of our schema management config will
 * be stored here.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_3_001__AddSchemaManagementConfigurationExternalTable : BaseJavaMigration() {
  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class AutoPropagationStatus(
    private val literal: String,
  ) : EnumType {
    enabled(ENABLED),
    disabled(DISABLED),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "auto_propagation_status"

    override fun getLiteral(): String = literal
  }

  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addAutoPropagationTypeEnum(ctx)
    addSchemaManagementTable(ctx)
    addIndexOnConnectionId(ctx)
  }

  companion object {
    private const val AUTO_PROPAGATION_STATUS = "auto_propagation_status"
    private const val ENABLED = "enabled"
    private const val DISABLED = "disabled"

    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false))
    private val AUTO_PROPAGATION_STATUS_COLUMN =
      DSL.field(
        "auto_propagation_status",
        SQLDataType.VARCHAR.asEnumDataType(AutoPropagationStatus::class.java).nullable(false),
      )
    private val CREATED_AT_COLUMN =
      DSL.field(
        "created_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(
          DSL.currentOffsetDateTime(),
        ),
      )
    private val UPDATED_AT_COLUMN =
      DSL.field(
        "updated_at",
        SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(
          DSL.currentOffsetDateTime(),
        ),
      )
    private val CONNECTION_ID_COLUMN = DSL.field("connection_id", SQLDataType.UUID.nullable(false))

    private fun addIndexOnConnectionId(ctx: DSLContext) {
      ctx
        .createIndexIfNotExists("connection_idx")
        .on(SCHEMA_MANAGEMENT_TABLE, CONNECTION_ID_COLUMN.name)
        .execute()
    }

    private fun addSchemaManagementTable(ctx: DSLContext) {
      ctx
        .createTableIfNotExists(SCHEMA_MANAGEMENT_TABLE)
        .columns(
          ID_COLUMN,
          AUTO_PROPAGATION_STATUS_COLUMN,
          CONNECTION_ID_COLUMN,
          CREATED_AT_COLUMN,
          UPDATED_AT_COLUMN,
        ).constraints(
          DSL.primaryKey(ID_COLUMN),
          DSL
            .foreignKey<UUID>(
              CONNECTION_ID_COLUMN,
            ).references(CONNECTION_TABLE, "id")
            .onDeleteCascade(),
        ).execute()
    }

    private fun addAutoPropagationTypeEnum(ctx: DSLContext) {
      ctx.createType(AUTO_PROPAGATION_STATUS).asEnum(ENABLED, DISABLED).execute()
    }
  }
}
