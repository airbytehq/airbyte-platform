/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;

/**
 * Query helpers that support {@link RepositoryTestSetup}.
 */
public class RepositoryTestSyncHelper {

  private final ExceptionWrappingDatabase database;

  public RepositoryTestSyncHelper(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Write standard sync (a.k.a. connection) to the db
   *
   * @param standardSync standard sync (a.k.a. connection)
   * @throws IOException exception when interacting with the db
   */
  public void createStandardSync(final StandardSync standardSync) throws IOException {
    database.transaction(ctx -> {
      createStandardSync(standardSync, ctx);
      return null;
    });
  }

  private void createStandardSync(final StandardSync standardSync, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
      throw new RuntimeException("unexpected schedule type mismatch");
    }

    ctx.insertInto(CONNECTION)
        .set(CONNECTION.ID, standardSync.getConnectionId())
        .set(CONNECTION.NAMESPACE_DEFINITION, Enums.toEnum(standardSync.getNamespaceDefinition().value(),
            io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType.class).orElseThrow())
        .set(CONNECTION.NAMESPACE_FORMAT, standardSync.getNamespaceFormat())
        .set(CONNECTION.PREFIX, standardSync.getPrefix())
        .set(CONNECTION.SOURCE_ID, standardSync.getSourceId())
        .set(CONNECTION.DESTINATION_ID, standardSync.getDestinationId())
        .set(CONNECTION.NAME, standardSync.getName())
        .set(CONNECTION.CATALOG, JSONB.valueOf(Jsons.serialize(standardSync.getCatalog())))
        .set(CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getFieldSelectionData())))
        .set(CONNECTION.STATUS, standardSync.getStatus() == null ? null
            : Enums.toEnum(standardSync.getStatus().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.StatusType.class).orElseThrow())
        .set(CONNECTION.SCHEDULE, JSONB.valueOf(Jsons.serialize(standardSync.getSchedule())))
        .set(CONNECTION.MANUAL, standardSync.getManual())
        .set(CONNECTION.SCHEDULE_TYPE,
            standardSync.getScheduleType() == null ? null
                : Enums.toEnum(standardSync.getScheduleType().value(),
                    io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType.class)
                    .orElseThrow())
        .set(CONNECTION.SCHEDULE_DATA, JSONB.valueOf(Jsons.serialize(standardSync.getScheduleData())))
        .set(CONNECTION.RESOURCE_REQUIREMENTS,
            JSONB.valueOf(Jsons.serialize(standardSync.getResourceRequirements())))
        .set(CONNECTION.SOURCE_CATALOG_ID, standardSync.getSourceCatalogId())
        .set(CONNECTION.DATAPLANE_GROUP_ID, standardSync.getDataplaneGroupId())
        .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
        .set(CONNECTION.CREATED_AT, timestamp)
        .set(CONNECTION.UPDATED_AT, timestamp)
        .execute();

    updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
    updateOrCreateSchemaChangeNotificationPreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(), timestamp,
        ctx);

    for (final UUID operationIdFromStandardSync : standardSync.getOperationIds()) {
      ctx.insertInto(CONNECTION_OPERATION)
          .set(CONNECTION_OPERATION.ID, UUID.randomUUID())
          .set(CONNECTION_OPERATION.CONNECTION_ID, standardSync.getConnectionId())
          .set(CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
          .set(CONNECTION_OPERATION.CREATED_AT, timestamp)
          .set(CONNECTION_OPERATION.UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  private void updateOrCreateNotificationConfiguration(final StandardSync standardSync, final OffsetDateTime timestamp, final DSLContext ctx) {
    final List<NotificationConfigurationRecord> notificationConfigurations = ctx.selectFrom(NOTIFICATION_CONFIGURATION)
        .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
        .fetch();
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.webhook, standardSync, timestamp, ctx);
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.email, standardSync, timestamp, ctx);
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private void updateOrCreateSchemaChangeNotificationPreference(final UUID connectionId,
                                                                final StandardSync.NonBreakingChangesPreference nonBreakingChangesPreference,
                                                                final OffsetDateTime timestamp,
                                                                final DSLContext ctx) {
    if (nonBreakingChangesPreference == null) {
      return;
    }
    final List<SchemaManagementRecord> schemaManagementConfigurations = ctx.selectFrom(SCHEMA_MANAGEMENT)
        .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .fetch();
    if (schemaManagementConfigurations.isEmpty()) {
      ctx.insertInto(SCHEMA_MANAGEMENT)
          .set(SCHEMA_MANAGEMENT.ID, UUID.randomUUID())
          .set(SCHEMA_MANAGEMENT.CONNECTION_ID, connectionId)
          .set(SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(SCHEMA_MANAGEMENT.CREATED_AT, timestamp)
          .set(SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .execute();
    } else if (schemaManagementConfigurations.size() == 1) {
      ctx.update(SCHEMA_MANAGEMENT)
          .set(SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS, AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()))
          .set(SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
          .where(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
          .execute();
    } else {
      throw new IllegalStateException("More than one schema management entry found for the connection: " + connectionId);
    }
  }

  /**
   * Check if an update has been made to an existing configuration and update the entry accordingly.
   * If no configuration exists, this will create an entry if the targetted notification type is being
   * enabled.
   */
  private void updateNotificationConfigurationIfNeeded(final List<NotificationConfigurationRecord> notificationConfigurations,
                                                       final NotificationType notificationType,
                                                       final StandardSync standardSync,
                                                       final OffsetDateTime timestamp,
                                                       final DSLContext ctx) {
    final Optional<NotificationConfigurationRecord> maybeConfiguration = notificationConfigurations.stream()
        .filter(notificationConfiguration -> notificationConfiguration.getNotificationType() == notificationType)
        .findFirst();

    if (maybeConfiguration.isPresent()) {
      if ((maybeConfiguration.get().getEnabled() && !standardSync.getNotifySchemaChanges())
          || (!maybeConfiguration.get().getEnabled() && standardSync.getNotifySchemaChanges())) {
        ctx.update(NOTIFICATION_CONFIGURATION)
            .set(NOTIFICATION_CONFIGURATION.ENABLED, getNotificationEnabled(standardSync, notificationType))
            .set(NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
            .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
            .and(NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE.eq(notificationType))
            .execute();
      }
    } else if (getNotificationEnabled(standardSync, notificationType)) {
      ctx.insertInto(NOTIFICATION_CONFIGURATION)
          .set(NOTIFICATION_CONFIGURATION.ID, UUID.randomUUID())
          .set(NOTIFICATION_CONFIGURATION.CONNECTION_ID, standardSync.getConnectionId())
          .set(NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE, notificationType)
          .set(NOTIFICATION_CONFIGURATION.ENABLED, true)
          .set(NOTIFICATION_CONFIGURATION.CREATED_AT, timestamp)
          .set(NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
          .execute();
    }
  }

  /**
   * Fetch if a notification is enabled in a standard sync based on the notification type.
   */
  @VisibleForTesting
  static boolean getNotificationEnabled(final StandardSync standardSync, final NotificationType notificationType) {
    switch (notificationType) {
      case webhook:
        return standardSync.getNotifySchemaChanges() != null && standardSync.getNotifySchemaChanges();
      case email:
        return standardSync.getNotifySchemaChangesByEmail() != null && standardSync.getNotifySchemaChangesByEmail();
      default:
        throw new IllegalStateException("Notification type unsupported");
    }
  }

}
