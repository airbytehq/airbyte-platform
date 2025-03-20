/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;

import com.google.common.annotations.VisibleForTesting;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_1_018__AddConfigTemplateAndPartialUserConfigTables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_018__AddConfigTemplateAndPartialUserConfigTables.class);

  // Table names
  static final String CONFIG_TEMPLATE_TABLE_NAME = "config_template";
  static final String PARTIAL_USER_CONFIG_TABLE_NAME = "partial_user_config";

  // Common field names
  static final String ID_FIELD = "id";
  static final String ORGANIZATION_ID_FIELD = "organization_id";
  static final String TOMBSTONE_FIELD = "tombstone";
  static final String CREATED_AT_FIELD = "created_at";
  static final String UPDATED_AT_FIELD = "updated_at";

  // Config templates field names
  static final String ACTOR_DEFINITION_ID_FIELD = "actor_definition_id";
  static final String PARTIAL_DEFAULT_CONFIG_FIELD = "partial_default_config";
  static final String USER_CONFIG_SPEC_FIELD = "user_config_spec";

  // Partial user configs field names
  static final String WORKSPACE_ID_FIELD = "workspace_id";
  static final String CONFIG_TEMPLATE_ID_FIELD = "config_template_id";
  static final String PARTIAL_USER_CONFIG_PROPERTIES_FIELD = "partial_user_config_properties";

  // Foreign key constraint names
  static final String CONFIG_TEMPLATE_ORG_FK = "config_template_organization_id_fkey";
  static final String CONFIG_TEMPLATE_ACTOR_FK = "config_template_actor_definition_id_fkey";
  static final String PARTIAL_USER_CONFIG_WORKSPACE_FK = "partial_user_config_workspace_id_fkey";
  static final String PARTIAL_USER_CONFIG_TEMPLATE_FK = "partial_user_config_template_id_fkey";

  // Referenced tables
  static final String ORGANIZATION_TABLE = "organization";
  static final String ACTOR_DEFINITION_VERSION_TABLE = "actor_definition_version";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    doMigration(ctx);
  }

  @VisibleForTesting
  static void doMigration(final DSLContext ctx) {
    createConfigTemplateTable(ctx);
    createPartialUserConfigTable(ctx);
  }

  @VisibleForTesting
  static void createConfigTemplateTable(final DSLContext ctx) {
    // Create config_template table
    ctx.createTableIfNotExists(CONFIG_TEMPLATE_TABLE_NAME)
        .column(ID_FIELD, SQLDataType.UUID.notNull())
        .column(ORGANIZATION_ID_FIELD, SQLDataType.UUID.notNull())
        .column(ACTOR_DEFINITION_ID_FIELD, SQLDataType.UUID.notNull())
        .column(PARTIAL_DEFAULT_CONFIG_FIELD, SQLDataType.JSONB.notNull())
        .column(USER_CONFIG_SPEC_FIELD, SQLDataType.JSONB.notNull())
        .column(TOMBSTONE_FIELD, SQLDataType.BOOLEAN.notNull().defaultValue(false))
        .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()))
        .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()))
        .primaryKey(ID_FIELD)
        .execute();

    // Add foreign key constraints for config_template
    ctx.alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(DSL.constraint(CONFIG_TEMPLATE_ORG_FK)
            .foreignKey(ORGANIZATION_ID_FIELD)
            .references(ORGANIZATION_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();

    ctx.alterTable(CONFIG_TEMPLATE_TABLE_NAME)
        .add(DSL.constraint(CONFIG_TEMPLATE_ACTOR_FK)
            .foreignKey(ACTOR_DEFINITION_ID_FIELD)
            .references(ACTOR_DEFINITION_VERSION_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();
  }

  @VisibleForTesting
  static void createPartialUserConfigTable(final DSLContext ctx) {
    // Create partial_user_config table
    ctx.createTableIfNotExists(PARTIAL_USER_CONFIG_TABLE_NAME)
        .column(ID_FIELD, SQLDataType.UUID.notNull())
        .column(WORKSPACE_ID_FIELD, SQLDataType.UUID.notNull())
        .column(CONFIG_TEMPLATE_ID_FIELD, SQLDataType.UUID.notNull())
        .column(PARTIAL_USER_CONFIG_PROPERTIES_FIELD, SQLDataType.JSONB.notNull())
        .column(TOMBSTONE_FIELD, SQLDataType.BOOLEAN.notNull().defaultValue(false))
        .column(CREATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()))
        .column(UPDATED_AT_FIELD, SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(currentOffsetDateTime()))
        .primaryKey(ID_FIELD)
        .execute();

    // Add foreign key constraints for partial_user_config
    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(DSL.constraint(PARTIAL_USER_CONFIG_WORKSPACE_FK)
            .foreignKey(WORKSPACE_ID_FIELD)
            .references(ORGANIZATION_TABLE, ID_FIELD)
            .onDeleteCascade())
        .execute();

    ctx.alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .add(DSL.constraint(PARTIAL_USER_CONFIG_TEMPLATE_FK)
            .foreignKey(CONFIG_TEMPLATE_ID_FIELD)
            .references(CONFIG_TEMPLATE_TABLE_NAME, ID_FIELD)
            .onDeleteCascade())
        .execute();
  }

}
