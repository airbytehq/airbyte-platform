/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add Connector Builder tables to the platform.
 */
public class V0_41_00_001__AddConnectorBuilderProjectTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_41_00_001__AddConnectorBuilderProjectTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addConnectorBuilderProjectTable(ctx);
  }

  private static void addConnectorBuilderProjectTable(final DSLContext ctx) {
    final Field<UUID> workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false));
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<String> name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false));
    final Field<JSONB> manifestDraft = DSL.field("manifest_draft", SQLDataType.JSONB.nullable(true));
    final Field<UUID> actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(true));
    final Field<Boolean> tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists("connector_builder_project")
        .columns(id, workspaceId, name, manifestDraft, actorDefinitionId, tombstone, createdAt, updatedAt)
        .constraints(primaryKey(id)).execute();
    ctx.createIndexIfNotExists("connector_builder_project_workspace_idx").on("connector_builder_project", "workspace_id").execute();
  }

}
