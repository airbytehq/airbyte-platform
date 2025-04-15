/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.migrations.V1_1_0_011__CreateConnectionTagTable
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_005__CreateDataPlaneTableAndDataPlaneGroupTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    migrate(ctx)
  }

  companion object {
    private const val DATAPLANE_TABLE = "dataplane"
    private const val DATAPLANE_GROUP_TABLE = "dataplane_group"
    private const val USER_TABLE = "user"
    private const val ORGANIZATION_TABLE = "organization"
    private const val ID_FIELD_NAME = "id"
    private const val NAME_FIELD_NAME = "name"
    private const val DATAPLANE_GROUP_ID_FIELD_NAME = "dataplane_group_id"
    private const val ORGANIZATION_ID_FIELD_NAME = "organization_id"
    private const val ENABLED_FIELD_NAME = "enabled"
    private const val CREATED_AT_FIELD_NAME = "created_at"
    private const val UPDATED_AT_FIELD_NAME = "updated_at"
    private const val UPDATED_BY_FIELD_NAME = "updated_by"
    private const val TOMBSTONE_FIELD_NAME = "tombstone"

    fun migrate(ctx: DSLContext) {
      createDataplaneGroupTable(ctx)
      createDataplaneTable(ctx)
    }

    private fun createDataplaneGroupTable(ctx: DSLContext) {
      val id = DSL.field(ID_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val organizationId = DSL.field(ORGANIZATION_ID_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val name = DSL.field(NAME_FIELD_NAME, SQLDataType.VARCHAR(255).nullable(false))
      val enabled = DSL.field(ENABLED_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(true))
      val createdAt = DSL.field(CREATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field(UPDATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedBy = DSL.field(UPDATED_BY_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val tombstone = DSL.field(TOMBSTONE_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(false))

      ctx
        .createTable(DATAPLANE_GROUP_TABLE)
        .columns(id, organizationId, name, enabled, createdAt, updatedAt, updatedBy, tombstone)
        .constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(organizationId).references(ORGANIZATION_TABLE, ID_FIELD_NAME).onDeleteCascade(),
          DSL.foreignKey(updatedBy).references(USER_TABLE, ID_FIELD_NAME).onDeleteCascade(),
          DSL.unique(organizationId, name),
        ).execute()
    }

    private fun createDataplaneTable(ctx: DSLContext) {
      val id = DSL.field(ID_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val dataplaneGroupId = DSL.field(DATAPLANE_GROUP_ID_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val name = DSL.field(NAME_FIELD_NAME, SQLDataType.VARCHAR(255).nullable(false))
      val enabled = DSL.field(ENABLED_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(true))
      val createdAt = DSL.field(CREATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field(UPDATED_AT_FIELD_NAME, SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedBy = DSL.field(UPDATED_BY_FIELD_NAME, SQLDataType.UUID.nullable(false))
      val tombstone = DSL.field(TOMBSTONE_FIELD_NAME, SQLDataType.BOOLEAN.nullable(false).defaultValue(false))

      ctx
        .createTable(DATAPLANE_TABLE)
        .columns(id, dataplaneGroupId, name, enabled, createdAt, updatedAt, updatedBy, tombstone)
        .constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(dataplaneGroupId).references(DATAPLANE_GROUP_TABLE, ID_FIELD_NAME).onDeleteCascade(),
          DSL.foreignKey(updatedBy).references(USER_TABLE, ID_FIELD_NAME).onDeleteCascade(),
          DSL.unique(dataplaneGroupId, name),
        ).execute()
    }
  }
}
