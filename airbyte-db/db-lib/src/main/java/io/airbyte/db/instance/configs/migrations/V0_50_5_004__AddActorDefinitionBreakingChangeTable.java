/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Date;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds a new table to the configs database to track connector breaking changes.
 */
public class V0_50_5_004__AddActorDefinitionBreakingChangeTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_5_004__AddActorDefinitionBreakingChangeTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createBreakingChangesTable(ctx);
  }

  @VisibleForTesting
  static void createBreakingChangesTable(final DSLContext ctx) {
    final Field<UUID> actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false));
    final Field<String> version = DSL.field("version", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> migrationDocumentationUrl = DSL.field("migration_documentation_url", SQLDataType.VARCHAR(256).nullable(false));
    final Field<Date> upgradeDeadline = DSL.field("upgrade_deadline", SQLDataType.DATE.nullable(false));
    final Field<String> message = DSL.field("message", SQLDataType.VARCHAR(256).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists("actor_definition_breaking_change")
        .primaryKey(actorDefinitionId, version)
        .constraint(foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade())
        .columns(actorDefinitionId,
            version,
            migrationDocumentationUrl,
            upgradeDeadline,
            message,
            createdAt,
            updatedAt)
        .execute();
  }

}
