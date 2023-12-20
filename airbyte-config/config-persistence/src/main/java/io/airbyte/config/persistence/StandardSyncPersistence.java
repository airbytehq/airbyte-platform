/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.STATE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.select;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigWithMetadata;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.data.services.impls.jooq.DbConverter;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus;
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.StreamDescriptor;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;

/**
 * All db queries for the StandardSync resource. Also known as a Connection.
 */
public class StandardSyncPersistence {

  private record StandardSyncIdsWithProtocolVersions(
                                                     UUID standardSyncId,
                                                     UUID sourceDefId,
                                                     Version sourceProtocolVersion,
                                                     UUID destinationDefId,
                                                     Version destinationProtocolVersion) {}

  private final ExceptionWrappingDatabase database;

  public StandardSyncPersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  public StandardSync getStandardSync(final UUID connectionId) throws IOException, ConfigNotFoundException {
    return getStandardSyncWithMetadata(connectionId).getConfig();
  }

  private ConfigWithMetadata<StandardSync> getStandardSyncWithMetadata(final UUID connectionId) throws IOException, ConfigNotFoundException {
    final List<ConfigWithMetadata<StandardSync>> result = listStandardSyncWithMetadata(Optional.of(connectionId));

    final boolean foundMoreThanOneConfig = result.size() > 1;
    if (result.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC, connectionId.toString());
    } else if (foundMoreThanOneConfig) {
      throw new IllegalStateException(String.format("Multiple %s configs found for ID %s: %s", ConfigSchema.STANDARD_SYNC, connectionId, result));
    }
    return result.get(0);
  }

  public List<StandardSync> listStandardSync() throws IOException {
    return listStandardSyncWithMetadata(Optional.empty()).stream().map(ConfigWithMetadata::getConfig).toList();
  }

  /**
   * Write standard sync (a.k.a. connection) to the db
   *
   * @param standardSync standard sync (a.k.a. connection)
   * @throws IOException exception when interacting with the db
   */
  public void writeStandardSync(final StandardSync standardSync) throws IOException {
    database.transaction(ctx -> {
      writeStandardSync(standardSync, ctx);
      return null;
    });
  }

  private void writeStandardSync(final StandardSync standardSync, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(CONNECTION)
        .where(CONNECTION.ID.eq(standardSync.getConnectionId())));

    if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
      throw new RuntimeException("unexpected schedule type mismatch");
    }

    if (isExistingConfig) {
      ctx.update(CONNECTION)
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
          .set(CONNECTION.UPDATED_AT, timestamp)
          .set(CONNECTION.SOURCE_CATALOG_ID, standardSync.getSourceCatalogId())
          .set(CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
          .set(CONNECTION.GEOGRAPHY, Enums.toEnum(standardSync.getGeography().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
          .where(CONNECTION.ID.eq(standardSync.getConnectionId()))
          .execute();

      updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx);
      updateOrCreateSchemaChangeNotificationPreference(standardSync.getConnectionId(), standardSync.getNonBreakingChangesPreference(), timestamp,
          ctx);

      ctx.deleteFrom(CONNECTION_OPERATION)
          .where(CONNECTION_OPERATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
          .execute();

      for (final UUID operationIdFromStandardSync : standardSync.getOperationIds()) {
        ctx.insertInto(CONNECTION_OPERATION)
            .set(CONNECTION_OPERATION.ID, UUID.randomUUID())
            .set(CONNECTION_OPERATION.CONNECTION_ID, standardSync.getConnectionId())
            .set(CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
            .set(CONNECTION_OPERATION.CREATED_AT, timestamp)
            .set(CONNECTION_OPERATION.UPDATED_AT, timestamp)
            .execute();
      }
    } else {
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
          .set(CONNECTION.GEOGRAPHY, Enums.toEnum(standardSync.getGeography().value(),
              io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
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

  /**
   * Deletes a connection (sync) and all of dependent resources (state and connection_operations).
   *
   * @param standardSyncId - id of the sync (a.k.a. connection_id)
   * @throws IOException - error while accessing db.
   */
  public void deleteStandardSync(final UUID standardSyncId) throws IOException {
    database.transaction(ctx -> {
      PersistenceHelpers.deleteConfig(NOTIFICATION_CONFIGURATION, NOTIFICATION_CONFIGURATION.CONNECTION_ID, standardSyncId, ctx);
      PersistenceHelpers.deleteConfig(CONNECTION_OPERATION, CONNECTION_OPERATION.CONNECTION_ID, standardSyncId, ctx);
      PersistenceHelpers.deleteConfig(STATE, STATE.CONNECTION_ID, standardSyncId, ctx);
      PersistenceHelpers.deleteConfig(CONNECTION, CONNECTION.ID, standardSyncId, ctx);
      return null;
    });
  }

  public List<StreamDescriptor> getAllStreamsForConnection(final UUID connectionId) throws ConfigNotFoundException, IOException {
    final StandardSync standardSync = getStandardSync(connectionId);
    return CatalogHelpers.extractStreamDescriptors(standardSync.getCatalog());
  }

  private List<ConfigWithMetadata<StandardSync>> listStandardSyncWithMetadata(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(CONNECTION.asterisk(),
          SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
          .from(CONNECTION)
          // The schema management can be non-existent for a connection id, thus we need to do a left join
          .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID));
      if (configId.isPresent()) {
        return query.where(CONNECTION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    final List<ConfigWithMetadata<StandardSync>> standardSyncs = new ArrayList<>();
    for (final Record record : result) {
      final List<NotificationConfigurationRecord> notificationConfigurationRecords = database.query(ctx -> {
        if (configId.isPresent()) {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(configId.get()))
              .fetch();
        } else {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .fetch();
        }
      });

      final StandardSync standardSync =
          DbConverter.buildStandardSync(record, connectionOperationIds(record.get(CONNECTION.ID)), notificationConfigurationRecords);
      if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
        throw new RuntimeException("unexpected schedule type mismatch");
      }
      standardSyncs.add(new ConfigWithMetadata<>(
          record.get(CONNECTION.ID).toString(),
          ConfigSchema.STANDARD_SYNC.name(),
          record.get(CONNECTION.CREATED_AT).toInstant(),
          record.get(CONNECTION.UPDATED_AT).toInstant(),
          standardSync));
    }
    return standardSyncs;
  }

  private List<UUID> connectionOperationIds(final UUID connectionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(CONNECTION_OPERATION)
        .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
        .fetch());

    final List<UUID> ids = new ArrayList<>();
    for (final Record record : result) {
      ids.add(record.get(CONNECTION_OPERATION.OPERATION_ID));
    }

    return ids;
  }

}
