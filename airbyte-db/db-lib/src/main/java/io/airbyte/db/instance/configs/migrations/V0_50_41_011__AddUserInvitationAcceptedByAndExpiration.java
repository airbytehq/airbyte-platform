/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.USER_INVITATION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.USER_TABLE;
import static org.jooq.impl.DSL.foreignKey;

import java.sql.Timestamp;
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
 * Add accepted_by_user_id column and expires_at column to user_invitations table. Also add expired
 * status to invitation_status enum.
 */
public class V0_50_41_011__AddUserInvitationAcceptedByAndExpiration extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_41_011__AddUserInvitationAcceptedByAndExpiration.class);

  private static final String ACCEPTED_BY_USER_ID = "accepted_by_user_id";
  private static final String EXPIRES_AT = "expires_at";
  private static final String INVITATION_STATUS = "invitation_status";
  private static final String EXPIRED = "expired";

  private static final Field<UUID> ACCEPTED_BY_USER_ID_COLUMN = DSL.field(ACCEPTED_BY_USER_ID, SQLDataType.UUID.nullable(true));
  private static final Field<Timestamp> EXPIRES_AT_COLUMN = DSL.field(EXPIRES_AT, SQLDataType.TIMESTAMP.nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());

    addAcceptedByUserIdColumnAndIndex(ctx);
    addExpiresAtColumnAndIndex(ctx);
    addExpiredStatus(ctx);
  }

  static void addAcceptedByUserIdColumnAndIndex(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE)
        .addColumnIfNotExists(ACCEPTED_BY_USER_ID_COLUMN)
        .execute();

    ctx.alterTable(USER_INVITATION_TABLE)
        .add(foreignKey(ACCEPTED_BY_USER_ID)
            .references(USER_TABLE, "id")
            .onDeleteCascade())
        .execute();

    ctx.createIndex("user_invitation_accepted_by_user_id_index")
        .on(USER_INVITATION_TABLE, ACCEPTED_BY_USER_ID)
        .execute();
  }

  static void addExpiresAtColumnAndIndex(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE).addColumnIfNotExists(EXPIRES_AT_COLUMN).execute();

    ctx.createIndex("user_invitation_expires_at_index")
        .on(USER_INVITATION_TABLE, EXPIRES_AT)
        .execute();
  }

  static void addExpiredStatus(final DSLContext ctx) {
    ctx.alterType(INVITATION_STATUS).addValue(EXPIRED).execute();
  }

}
