/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.CONNECTION_TABLE
import io.airbyte.db.instance.DatabaseConstants.NOTIFICATION_CONFIGURATION_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Record1
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.time.OffsetDateTime
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Miration that extract the notification configuration from a column to a dedicated table. It will:
 *
 *  1. Create a new enum type for the notification type
 *  1. Create the new table
 *  1. Migrate the existing values from the connection table to the new one
 *  1. Delete the deprecated column
 *
 */
@Suppress("ktlint:standard:class-naming")
class V0_41_02_002__AddNotificationConfigurationExternalTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addNotificationTypeEnum(ctx)
    addNotificationConfigurationTable(ctx)
    migrateExistingNotificationConfigs(ctx)
    dropDeprecatedConfigColumn(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class NotificationType(
    private val literal: String,
  ) : EnumType {
    webhook("webhook"),
    email("email"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "notification_type"

    override fun getLiteral(): String = literal
  }

  companion object {
    /**
     * Create the notification type enum.
     */
    private fun addNotificationTypeEnum(ctx: DSLContext) {
      ctx.createType("notification_type").asEnum("webhook", "email").execute()
    }

    private val id = DSL.field("id", SQLDataType.UUID.nullable(false))
    private val enabled = DSL.field("enabled", SQLDataType.BOOLEAN.nullable(false))
    private val notificationType =
      DSL.field(
        "notification_type",
        SQLDataType.VARCHAR
          .asEnumDataType(
            NotificationType::class.java,
          ).nullable(false),
      )
    private val connectionIdField = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
    private val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
    private val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    /**
     * Create the notification configuration table.
     */
    private fun addNotificationConfigurationTable(ctx: DSLContext) {
      ctx
        .createTableIfNotExists(NOTIFICATION_CONFIGURATION_TABLE)
        .columns(id, enabled, notificationType, connectionIdField, createdAt, updatedAt)
        .constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(connectionIdField).references(CONNECTION_TABLE, "id").onDeleteCascade(),
        ).execute()
    }

    /**
     * Migrate the existing configuration to the new table.
     */
    private fun migrateExistingNotificationConfigs(ctx: DSLContext) {
      val connectionWithSlackNotification =
        ctx
          .select(DSL.field("id"))
          .from(CONNECTION_TABLE)
          .where(DSL.field("notify_schema_changes").isTrue())
          .fetch()
          .map { record: Record1<Any> -> record.get(0, UUID::class.java) }

      connectionWithSlackNotification.forEach { connectionId ->
        val now = OffsetDateTime.now()
        ctx
          .insertInto(DSL.table(NOTIFICATION_CONFIGURATION_TABLE))
          .set(id, UUID.randomUUID())
          .set(enabled, true)
          .set(notificationType, NotificationType.webhook)
          .set(connectionIdField, connectionId)
          .set(createdAt, now)
          .set(updatedAt, now)
          .execute()
      }
    }

    /**
     * Drop the old column.
     */
    private fun dropDeprecatedConfigColumn(ctx: DSLContext) {
      ctx
        .alterTable(CONNECTION_TABLE)
        .dropColumn(DSL.field("notify_schema_changes"))
        .execute()
    }
  }
}
