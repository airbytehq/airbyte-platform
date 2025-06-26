/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.NamespaceDefinitionType
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_004__AddConnectionTemplateTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)
    doMigration(ctx)
  }

  @VisibleForTesting
  fun doMigration(ctx: DSLContext) {
    ctx
      .createTableIfNotExists(CONNECTION_TEMPLATE_TABLE)
      .column(ID_FIELD, SQLDataType.UUID.notNull())
      .column(ORGANIZATION_ID_FIELD, SQLDataType.UUID.notNull())
      .column(DESTINATION_NAME, SQLDataType.VARCHAR.notNull())
      .column(DESTINATION_DEFINITION_ID_FIELD, SQLDataType.UUID.notNull())
      .column(DESTINATION_CONFIG, SQLDataType.JSONB.notNull())
      .column(
        NAMESPACE_DEFINITION,
        SQLDataType.VARCHAR
          .asEnumDataType(
            NamespaceDefinitionType::class.java,
          ).notNull(),
      ).column(NAMESPACE_FORMAT_FIELD, SQLDataType.VARCHAR)
      .column(PREFIX_FIELD, SQLDataType.VARCHAR)
      .column(
        SCHEDULE_TYPE_FIELD,
        SQLDataType.VARCHAR.asEnumDataType(V0_36_3_001__AddScheduleTypeToConfigsTable.ScheduleType::class.java).notNull(),
      ).column(
        SCHEDULE_DATA_FIELD,
        SQLDataType.JSONB,
      ).column(
        RESOURCE_REQUIREMENTS_FIELD,
        SQLDataType.JSONB,
      ).column(
        NON_BREAKING_CHANGE_PREFERENCE_FIELD,
        SQLDataType.VARCHAR
          .asEnumDataType(
            V0_40_11_002__AddSchemaChangeColumnsToConnections.NonBreakingChangePreferenceType::class.java,
          ).notNull(),
      ).column(
        DATAPLANE_GROUP_ID_FIELD,
        SQLDataType.UUID.notNull(),
      ).column(TOMBSTONE_FIELD, SQLDataType.BOOLEAN.notNull().defaultValue(false))
      .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
      .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
      .primaryKey(ID_FIELD)
      .execute()

    // Add foreign key constraints
    ctx.alterTable(CONNECTION_TEMPLATE_TABLE).add(DSL.foreignKey(ORGANIZATION_ID_FIELD).references(ORGANIZATION_TABLE).onDeleteCascade()).execute()
    ctx
      .alterTable(
        CONNECTION_TEMPLATE_TABLE,
      ).add(DSL.foreignKey(DESTINATION_DEFINITION_ID_FIELD).references(ACTOR_DEFINITION_TABLE).onDeleteCascade())
      .execute()

    ctx.createIndexIfNotExists(INDEX_ON_ORGANIZATION_ID).on(CONNECTION_TEMPLATE_TABLE, ORGANIZATION_ID_FIELD).execute()
  }

  companion object {
    private const val CONNECTION_TEMPLATE_TABLE: String = "connection_template"
    private const val ACTOR_DEFINITION_TABLE: String = "actor_definition"
    private const val ORGANIZATION_TABLE: String = "organization"

    private const val INDEX_ON_ORGANIZATION_ID: String = "connection_template_organization_id_idx"

    private const val ID_FIELD: String = "id"
    private const val ORGANIZATION_ID_FIELD: String = "organization_id"
    private const val DESTINATION_NAME: String = "destination_name"

    private const val DESTINATION_DEFINITION_ID_FIELD: String = "destination_definition_id"
    private const val DESTINATION_CONFIG: String = "destination_config"
    private const val NAMESPACE_DEFINITION: String = "namespace_definition"
    private const val NAMESPACE_FORMAT_FIELD: String = "namespace_format"
    private const val PREFIX_FIELD: String = "prefix"
    private const val SCHEDULE_TYPE_FIELD: String = "schedule_type"
    private const val SCHEDULE_DATA_FIELD: String = "schedule_data"
    private const val RESOURCE_REQUIREMENTS_FIELD: String = "resource_requirements"
    private const val NON_BREAKING_CHANGE_PREFERENCE_FIELD: String = "non_breaking_change_preference"
    private const val DATAPLANE_GROUP_ID_FIELD: String = "dataplace_group_id"
    private const val TOMBSTONE_FIELD: String = "tombstone"
    private const val CREATED_AT_FIELD: String = "created_at"
    private const val UPDATED_AT_FIELD: String = "updated_at"
  }
}
