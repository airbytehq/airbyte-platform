/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE;
import static io.airbyte.db.instance.DatabaseConstants.NOTIFICATION_CONFIGURATION_TABLE;
import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Schema;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Miration that extract the notification configuration from a column to a dedicated table. It will:
 * <ol>
 * <li>Create a new enum type for the notification type</li>
 * <li>Create the new table</li>
 * <li>Migrate the existing values from the connection table to the new one</li>
 * <li>Delete the deprecated column</li>
 * </ol>
 */
public class V0_41_02_002__AddNotificationConfigurationExternalTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_41_02_002__AddNotificationConfigurationExternalTable.class);

  /**
   * Run the db migration.
   */
  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    addNotificationTypeEnum(ctx);
    addNotificationConfigurationTable(ctx);
    migrateExistingNotificationConfigs(ctx);
    dropDeprecatedConfigColumn(ctx);
  }

  /**
   * Create the notification type enum.
   */
  private static void addNotificationTypeEnum(final DSLContext ctx) {
    ctx.createType("notification_type").asEnum("webhook", "email").execute();
  }

  private static final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
  private static final Field<Boolean> enabled = DSL.field("enabled", SQLDataType.BOOLEAN.nullable(false));
  private static final Field<NotificationType> notificationType =
      DSL.field("notification_type", SQLDataType.VARCHAR.asEnumDataType(NotificationType.class).nullable(false));
  private static final Field<UUID> connectionIdField = DSL.field("connection_id", SQLDataType.UUID.nullable(false));
  private static final Field<OffsetDateTime> createdAt =
      DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
  private static final Field<OffsetDateTime> updatedAt =
      DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

  /**
   * Create the notification configuration table.
   */
  private static void addNotificationConfigurationTable(final DSLContext ctx) {
    ctx.createTableIfNotExists(NOTIFICATION_CONFIGURATION_TABLE)
        .columns(id, enabled, notificationType, connectionIdField, createdAt, updatedAt)
        .constraints(primaryKey(id),
            foreignKey(connectionIdField).references(CONNECTION_TABLE, "id").onDeleteCascade())
        .execute();
  }

  /**
   * Migrate the existing configuration to the new table.
   */
  private static void migrateExistingNotificationConfigs(final DSLContext ctx) {
    final List<UUID> connectionWithSlackNotification = ctx.select(DSL.field("id"))
        .from(CONNECTION_TABLE)
        .where(DSL.field("notify_schema_changes").isTrue())
        .fetch()
        .map(record -> record.get(0, UUID.class));

    connectionWithSlackNotification.forEach(connectionId -> {
      final OffsetDateTime now = OffsetDateTime.now();
      ctx.insertInto(DSL.table(NOTIFICATION_CONFIGURATION_TABLE))
          .set(id, UUID.randomUUID())
          .set(enabled, true)
          .set(notificationType, NotificationType.webhook)
          .set(connectionIdField, connectionId)
          .set(createdAt, now)
          .set(updatedAt, now)
          .execute();
    });
  }

  /**
   * Drop the old column.
   */
  private static void dropDeprecatedConfigColumn(final DSLContext ctx) {
    ctx.alterTable(CONNECTION_TABLE)
        .dropColumn(DSL.field(
            "notify_schema_changes"))
        .execute();
  }

  enum NotificationType implements EnumType {

    webhook("webhook"),
    email("email");

    private final String literal;

    NotificationType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return "notification_type";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
