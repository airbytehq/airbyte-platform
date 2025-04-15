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
class V1_1_1_018__AddConfigTemplateAndPartialUserConfigTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  companion object {
    // Table names
    const val CONFIG_TEMPLATE_TABLE_NAME: String = "config_template"
    const val PARTIAL_USER_CONFIG_TABLE_NAME: String = "partial_user_config"

    // Common field names
    const val ID_FIELD: String = "id"
    const val ORGANIZATION_ID_FIELD: String = "organization_id"
    const val TOMBSTONE_FIELD: String = "tombstone"
    const val CREATED_AT_FIELD: String = "created_at"
    const val UPDATED_AT_FIELD: String = "updated_at"

    // Config templates field names
    const val ACTOR_DEFINITION_ID_FIELD: String = "actor_definition_id"
    const val PARTIAL_DEFAULT_CONFIG_FIELD: String = "partial_default_config"
    const val USER_CONFIG_SPEC_FIELD: String = "user_config_spec"

    // Partial user configs field names
    const val WORKSPACE_ID_FIELD: String = "workspace_id"
    const val CONFIG_TEMPLATE_ID_FIELD: String = "config_template_id"
    const val PARTIAL_USER_CONFIG_PROPERTIES_FIELD: String = "partial_user_config_properties"

    // Foreign key constraint names
    const val CONFIG_TEMPLATE_ORG_FK: String = "config_template_organization_id_fkey"
    const val CONFIG_TEMPLATE_ACTOR_FK: String = "config_template_actor_definition_id_fkey"
    const val PARTIAL_USER_CONFIG_WORKSPACE_FK: String = "partial_user_config_workspace_id_fkey"
    const val PARTIAL_USER_CONFIG_TEMPLATE_FK: String = "partial_user_config_template_id_fkey"

    // Referenced tables
    const val ORGANIZATION_TABLE: String = "organization"
    const val ACTOR_DEFINITION_VERSION_TABLE: String = "actor_definition_version"

    @VisibleForTesting
    fun doMigration(ctx: DSLContext) {
      createConfigTemplateTable(ctx)
      createPartialUserConfigTable(ctx)
    }

    @VisibleForTesting
    fun createConfigTemplateTable(ctx: DSLContext) {
      // Create config_template table
      ctx
        .createTableIfNotExists(CONFIG_TEMPLATE_TABLE_NAME)
        .column(ID_FIELD, SQLDataType.UUID.notNull())
        .column(ORGANIZATION_ID_FIELD, SQLDataType.UUID.notNull())
        .column(ACTOR_DEFINITION_ID_FIELD, SQLDataType.UUID.notNull())
        .column(PARTIAL_DEFAULT_CONFIG_FIELD, SQLDataType.JSONB.notNull())
        .column(USER_CONFIG_SPEC_FIELD, SQLDataType.JSONB.notNull())
        .column(TOMBSTONE_FIELD, SQLDataType.BOOLEAN.notNull().defaultValue(false))
        .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        .primaryKey(ID_FIELD)
        .execute()

      // Add foreign key constraints for config_template
      ctx
        .alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(
          DSL
            .constraint(CONFIG_TEMPLATE_ORG_FK)
            .foreignKey(ORGANIZATION_ID_FIELD)
            .references(ORGANIZATION_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()

      ctx
        .alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(
          DSL
            .constraint(CONFIG_TEMPLATE_ACTOR_FK)
            .foreignKey(ACTOR_DEFINITION_ID_FIELD)
            .references(ACTOR_DEFINITION_VERSION_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()
    }

    @VisibleForTesting
    fun createPartialUserConfigTable(ctx: DSLContext) {
      // Create partial_user_config table
      ctx
        .createTableIfNotExists(PARTIAL_USER_CONFIG_TABLE_NAME)
        .column(ID_FIELD, SQLDataType.UUID.notNull())
        .column(WORKSPACE_ID_FIELD, SQLDataType.UUID.notNull())
        .column(CONFIG_TEMPLATE_ID_FIELD, SQLDataType.UUID.notNull())
        .column(PARTIAL_USER_CONFIG_PROPERTIES_FIELD, SQLDataType.JSONB.notNull())
        .column(TOMBSTONE_FIELD, SQLDataType.BOOLEAN.notNull().defaultValue(false))
        .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
        .primaryKey(ID_FIELD)
        .execute()

      // Add foreign key constraints for partial_user_config
      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(
          DSL
            .constraint(PARTIAL_USER_CONFIG_WORKSPACE_FK)
            .foreignKey(WORKSPACE_ID_FIELD)
            .references(ORGANIZATION_TABLE, ID_FIELD)
            .onDeleteCascade(),
        ).execute()

      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(
          DSL
            .constraint(PARTIAL_USER_CONFIG_TEMPLATE_FK)
            .foreignKey(CONFIG_TEMPLATE_ID_FIELD)
            .references(CONFIG_TEMPLATE_TABLE_NAME, ID_FIELD)
            .onDeleteCascade(),
        ).execute()
    }
  }
}
