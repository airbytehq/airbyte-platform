/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some dataplanes and dataplane groups will be created by the system and/or non-person entities, so
 * a foreign key to the user table isn't sufficient to provide audit information. Dropping these
 * columns and will revisit the audit strategy later on.
 */
public class V1_1_1_023__DropUserForeignKeysFromDataplaneTables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_023__DropUserForeignKeysFromDataplaneTables.class);
  private static final Table<Record> DATAPLANE_TABLE = DSL.table("dataplane");
  private static final Table<Record> DATAPLANE_GROUP_TABLE = DSL.table("dataplane_group");
  private static final Table<Record> DATAPLANE_CLIENT_CREDENTIALS_TABLE = DSL.table("dataplane_client_credentials");
  private static final Field<UUID> UPDATED_BY = DSL.field("updated_by", SQLDataType.UUID);
  private static final Field<UUID> CREATED_BY = DSL.field("created_by", SQLDataType.UUID);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    dropDataplaneUpdatedBy(ctx);
    dropDataplaneGroupUpdatedBy(ctx);
    dropDataplaneClientCredentialsCreatedBy(ctx);
  }

  static void dropDataplaneUpdatedBy(final DSLContext ctx) {
    LOGGER.info("Dropping 'updated_by' column from Dataplane table");
    ctx.alterTable(DATAPLANE_TABLE)
        .dropColumnIfExists(UPDATED_BY)
        .execute();
  }

  static void dropDataplaneGroupUpdatedBy(final DSLContext ctx) {
    LOGGER.info("Dropping 'updated_by' column from DataplaneGroup table");
    ctx.alterTable(DATAPLANE_GROUP_TABLE)
        .dropColumnIfExists(UPDATED_BY)
        .execute();
  }

  static void dropDataplaneClientCredentialsCreatedBy(final DSLContext ctx) {
    LOGGER.info("Dropping 'created_by' column from DataplaneClientCredentials table");
    ctx.alterTable(DATAPLANE_CLIENT_CREDENTIALS_TABLE)
        .dropColumnIfExists(CREATED_BY)
        .execute();
  }

}
