/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.FieldSelectionData
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.Schedule
import io.airbyte.config.ScheduleData
import io.airbyte.config.StandardSync
import io.airbyte.config.helpers.ScheduleHelpers.isScheduleTypeMismatch
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.AutoPropagationStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.NotificationType
import io.airbyte.db.instance.configs.jooq.generated.enums.ScheduleType
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord
import io.airbyte.db.instance.configs.jooq.generated.tables.records.SchemaManagementRecord
import org.jooq.DSLContext
import org.jooq.JSONB
import java.io.IOException
import java.time.OffsetDateTime
import java.util.UUID

/**
 * Query helpers that support [RepositoryTestSetup].
 */
class RepositoryTestSyncHelper(
  database: Database?,
) {
  private val database: ExceptionWrappingDatabase

  init {
    this.database = ExceptionWrappingDatabase(database)
  }

  /**
   * Write standard sync (a.k.a. connection) to the db
   *
   * @param standardSync standard sync (a.k.a. connection)
   * @throws IOException exception when interacting with the db
   */
  @Throws(IOException::class)
  fun createStandardSync(standardSync: StandardSync) {
    database.transaction<Any?>(
      ContextQueryFunction { ctx: DSLContext? ->
        createStandardSync(standardSync, ctx!!)
        null
      },
    )
  }

  private fun createStandardSync(
    standardSync: StandardSync,
    ctx: DSLContext,
  ) {
    val timestamp = OffsetDateTime.now()

    if (isScheduleTypeMismatch(standardSync)) {
      throw RuntimeException("unexpected schedule type mismatch")
    }

    ctx
      .insertInto(Tables.CONNECTION)
      .set(Tables.CONNECTION.ID, standardSync.getConnectionId())
      .set(
        Tables.CONNECTION.NAMESPACE_DEFINITION,
        standardSync.getNamespaceDefinition().value().toEnum<NamespaceDefinitionType>()
          ?: throw IllegalArgumentException("Invalid namespace definition type: " + standardSync.getNamespaceDefinition()),
      ).set(Tables.CONNECTION.NAMESPACE_FORMAT, standardSync.getNamespaceFormat())
      .set(Tables.CONNECTION.PREFIX, standardSync.getPrefix())
      .set(Tables.CONNECTION.SOURCE_ID, standardSync.getSourceId())
      .set(Tables.CONNECTION.DESTINATION_ID, standardSync.getDestinationId())
      .set(Tables.CONNECTION.NAME, standardSync.getName())
      .set(Tables.CONNECTION.CATALOG, JSONB.valueOf(serialize<ConfiguredAirbyteCatalog?>(standardSync.getCatalog())))
      .set(Tables.CONNECTION.FIELD_SELECTION_DATA, JSONB.valueOf(serialize<FieldSelectionData?>(standardSync.getFieldSelectionData())))
      .set(
        Tables.CONNECTION.STATUS,
        if (standardSync.getStatus() == null) {
          null
        } else {
          standardSync.getStatus().value().toEnum<StatusType>()
            ?: throw IllegalArgumentException("Invalid status type: " + standardSync.getStatus())
        },
      ).set(Tables.CONNECTION.SCHEDULE, JSONB.valueOf(serialize<Schedule?>(standardSync.getSchedule())))
      .set(Tables.CONNECTION.MANUAL, standardSync.getManual())
      .set(
        Tables.CONNECTION.SCHEDULE_TYPE,
        if (standardSync.getScheduleType() == null) {
          null
        } else {
          standardSync.getScheduleType().value().toEnum<ScheduleType>()
            ?: throw IllegalArgumentException("Invalid schedule type: " + standardSync.getScheduleType())
        },
      ).set(Tables.CONNECTION.SCHEDULE_DATA, JSONB.valueOf(serialize<ScheduleData?>(standardSync.getScheduleData())))
      .set(
        Tables.CONNECTION.RESOURCE_REQUIREMENTS,
        JSONB.valueOf(serialize<ResourceRequirements?>(standardSync.getResourceRequirements())),
      ).set(Tables.CONNECTION.SOURCE_CATALOG_ID, standardSync.getSourceCatalogId())
      .set(Tables.CONNECTION.BREAKING_CHANGE, standardSync.getBreakingChange())
      .set(Tables.CONNECTION.CREATED_AT, timestamp)
      .set(Tables.CONNECTION.UPDATED_AT, timestamp)
      .execute()

    updateOrCreateNotificationConfiguration(standardSync, timestamp, ctx)
    updateOrCreateSchemaChangeNotificationPreference(
      standardSync.getConnectionId(),
      standardSync.getNonBreakingChangesPreference(),
      timestamp,
      ctx,
    )

    for (operationIdFromStandardSync in standardSync.getOperationIds()) {
      ctx
        .insertInto(Tables.CONNECTION_OPERATION)
        .set(Tables.CONNECTION_OPERATION.ID, UUID.randomUUID())
        .set(Tables.CONNECTION_OPERATION.CONNECTION_ID, standardSync.getConnectionId())
        .set(Tables.CONNECTION_OPERATION.OPERATION_ID, operationIdFromStandardSync)
        .set(Tables.CONNECTION_OPERATION.CREATED_AT, timestamp)
        .set(Tables.CONNECTION_OPERATION.UPDATED_AT, timestamp)
        .execute()
    }
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  private fun updateOrCreateNotificationConfiguration(
    standardSync: StandardSync,
    timestamp: OffsetDateTime?,
    ctx: DSLContext,
  ) {
    val notificationConfigurations: MutableList<NotificationConfigurationRecord?> =
      ctx
        .selectFrom(Tables.NOTIFICATION_CONFIGURATION)
        .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
        .fetch()
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.webhook, standardSync, timestamp, ctx)
    updateNotificationConfigurationIfNeeded(notificationConfigurations, NotificationType.email, standardSync, timestamp, ctx)
  }

  /**
   * Update the notification configuration for a give connection (StandardSync). It needs to have the
   * standard sync to be persisted before being called because one column of the configuration is a
   * foreign key on the Connection Table.
   */
  private fun updateOrCreateSchemaChangeNotificationPreference(
    connectionId: UUID?,
    nonBreakingChangesPreference: StandardSync.NonBreakingChangesPreference?,
    timestamp: OffsetDateTime?,
    ctx: DSLContext,
  ) {
    if (nonBreakingChangesPreference == null) {
      return
    }
    val schemaManagementConfigurations: MutableList<SchemaManagementRecord?> =
      ctx
        .selectFrom(Tables.SCHEMA_MANAGEMENT)
        .where(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .fetch()
    if (schemaManagementConfigurations.isEmpty()) {
      ctx
        .insertInto(Tables.SCHEMA_MANAGEMENT)
        .set(Tables.SCHEMA_MANAGEMENT.ID, UUID.randomUUID())
        .set(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID, connectionId)
        .set(
          Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
          AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()),
        ).set(Tables.SCHEMA_MANAGEMENT.CREATED_AT, timestamp)
        .set(Tables.SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
        .execute()
    } else if (schemaManagementConfigurations.size == 1) {
      ctx
        .update(Tables.SCHEMA_MANAGEMENT)
        .set(
          Tables.SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS,
          AutoPropagationStatus.valueOf(nonBreakingChangesPreference.value()),
        ).set(Tables.SCHEMA_MANAGEMENT.UPDATED_AT, timestamp)
        .where(Tables.SCHEMA_MANAGEMENT.CONNECTION_ID.eq(connectionId))
        .execute()
    } else {
      throw IllegalStateException("More than one schema management entry found for the connection: " + connectionId)
    }
  }

  /**
   * Check if an update has been made to an existing configuration and update the entry accordingly.
   * If no configuration exists, this will create an entry if the targetted notification type is being
   * enabled.
   */
  private fun updateNotificationConfigurationIfNeeded(
    notificationConfigurations: MutableList<NotificationConfigurationRecord?>,
    notificationType: NotificationType,
    standardSync: StandardSync,
    timestamp: OffsetDateTime?,
    ctx: DSLContext,
  ) {
    val maybeConfiguration =
      notificationConfigurations
        .stream()
        .filter { notificationConfiguration: NotificationConfigurationRecord? ->
          notificationConfiguration!!.getNotificationType() ==
            notificationType
        }.findFirst()

    if (maybeConfiguration.isPresent()) {
      if ((maybeConfiguration.get().getEnabled() && !standardSync.getNotifySchemaChanges()) ||
        (!maybeConfiguration.get().getEnabled() && standardSync.getNotifySchemaChanges())
      ) {
        ctx
          .update(Tables.NOTIFICATION_CONFIGURATION)
          .set(Tables.NOTIFICATION_CONFIGURATION.ENABLED, getNotificationEnabled(standardSync, notificationType))
          .set(Tables.NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
          .where(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(standardSync.getConnectionId()))
          .and(Tables.NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE.eq(notificationType))
          .execute()
      }
    } else if (getNotificationEnabled(standardSync, notificationType)) {
      ctx
        .insertInto(Tables.NOTIFICATION_CONFIGURATION)
        .set(Tables.NOTIFICATION_CONFIGURATION.ID, UUID.randomUUID())
        .set(Tables.NOTIFICATION_CONFIGURATION.CONNECTION_ID, standardSync.getConnectionId())
        .set(Tables.NOTIFICATION_CONFIGURATION.NOTIFICATION_TYPE, notificationType)
        .set(Tables.NOTIFICATION_CONFIGURATION.ENABLED, true)
        .set(Tables.NOTIFICATION_CONFIGURATION.CREATED_AT, timestamp)
        .set(Tables.NOTIFICATION_CONFIGURATION.UPDATED_AT, timestamp)
        .execute()
    }
  }

  companion object {
    /**
     * Fetch if a notification is enabled in a standard sync based on the notification type.
     */
    @VisibleForTesting
    fun getNotificationEnabled(
      standardSync: StandardSync,
      notificationType: NotificationType,
    ): Boolean {
      when (notificationType) {
        NotificationType.webhook -> return standardSync.getNotifySchemaChanges() != null && standardSync.getNotifySchemaChanges()
        NotificationType.email -> return standardSync.getNotifySchemaChangesByEmail() != null && standardSync.getNotifySchemaChangesByEmail()
        else -> throw IllegalStateException("Notification type unsupported")
      }
    }
  }
}
