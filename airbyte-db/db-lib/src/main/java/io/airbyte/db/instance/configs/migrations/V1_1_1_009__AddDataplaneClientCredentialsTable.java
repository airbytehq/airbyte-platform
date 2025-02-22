/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

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

public class V1_1_1_009__AddDataplaneClientCredentialsTable extends BaseJavaMigration {

  private static final String DATAPLANE_CLIENT_CREDENTIALS_TABLE = "dataplane_client_credentials";
  private static final String DATAPLANE_TABLE = "dataplane";

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_009__AddDataplaneClientCredentialsTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());

    migrate(ctx);
  }

  public static void migrate(final DSLContext ctx) {
    createDataplaneAuthServiceTable(ctx);
  }

  private static void createDataplaneAuthServiceTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> dataplaneId = DSL.field("dataplane_id", SQLDataType.UUID.nullable(false));
    final Field<String> clientId = DSL.field("client_id", SQLDataType.VARCHAR.nullable(false));
    final Field<String> clientSecret = DSL.field("client_secret", SQLDataType.VARCHAR.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<UUID> createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false));

    ctx.createTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .columns(id, dataplaneId, clientId, clientSecret, createdAt, createdBy)
        .constraints(
            primaryKey(id),
            foreignKey(dataplaneId).references(DATAPLANE_TABLE, "id").onDeleteCascade(),
            foreignKey(createdBy).references("user", "id").onDeleteCascade(),
            unique(dataplaneId, clientId, clientSecret))
        .execute();
  }

}
