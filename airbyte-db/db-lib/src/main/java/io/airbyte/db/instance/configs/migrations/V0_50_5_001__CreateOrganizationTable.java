/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;

import com.google.common.annotations.VisibleForTesting;
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
 * Insert new table for organization settings. Subsequent migrations will add an organization_id
 * column to the workspace table.
 */
public class V0_50_5_001__CreateOrganizationTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_5_001__CreateOrganizationTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createOrganization(ctx);
  }

  @VisibleForTesting
  static void createOrganization(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<String> name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false));
    final Field<UUID> userId = DSL.field("user_id", SQLDataType.UUID.nullable(true));
    final Field<String> email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists("organization")
        .columns(id,
            name,
            userId,
            email,
            createdAt,
            updatedAt)
        .constraints(primaryKey(id))
        .execute();

    LOGGER.info("organization table created");
  }

}
