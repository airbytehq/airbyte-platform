/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.DATAPLANE_NETWORK_CONFIG_TABLE
import io.airbyte.db.instance.DatabaseConstants.PRIVATE_LINK_TABLE
import io.airbyte.db.instance.DatabaseConstants.WORKSPACE_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.DefaultDataType
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_018__CreatePrivateLinkTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx: DSLContext = DSL.using(context.connection)

    createCloudProviderEnumType(ctx)
    createPrivateLinkStatusEnumType(ctx)
    createPrivateLinkTableAndIndexes(ctx)
    createDataplaneNetworkConfigTable(ctx)
  }

  enum class CloudProvider(
    private val literal: String,
  ) : EnumType {
    AWS("aws"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "cloud_provider"
    }
  }

  enum class PrivateLinkStatus(
    private val literal: String,
  ) : EnumType {
    CREATING("creating"),
    PENDING_ACCEPTANCE("pending_acceptance"),
    CONFIGURING("configuring"),
    AVAILABLE("available"),
    CREATE_FAILED("create_failed"),
    DELETING("deleting"),
    DELETE_FAILED("delete_failed"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "private_link_status"
    }
  }

  companion object {
    private fun createCloudProviderEnumType(ctx: DSLContext) {
      ctx
        .createType(CloudProvider.NAME)
        .asEnum(*CloudProvider.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createPrivateLinkStatusEnumType(ctx: DSLContext) {
      ctx
        .createType(PrivateLinkStatus.NAME)
        .asEnum(*PrivateLinkStatus.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createPrivateLinkTableAndIndexes(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val dataplaneGroupId = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(63).nullable(false))
      val status =
        DSL.field(
          "status",
          SQLDataType.VARCHAR
            .asEnumDataType(PrivateLinkStatus::class.java)
            .nullable(false)
            .defaultValue(PrivateLinkStatus.CREATING),
        )
      val serviceRegion = DSL.field("service_region", SQLDataType.VARCHAR(64).nullable(false))
      val serviceName = DSL.field("service_name", SQLDataType.VARCHAR(512).nullable(false))
      val endpointId = DSL.field("endpoint_id", SQLDataType.VARCHAR(512).nullable(true))
      val dnsName = DSL.field("dns_name", SQLDataType.VARCHAR(512).nullable(true))
      val scopedConfigurationId = DSL.field("scoped_configuration_id", SQLDataType.UUID.nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(PRIVATE_LINK_TABLE)
        .columns(
          id,
          workspaceId,
          dataplaneGroupId,
          name,
          status,
          serviceRegion,
          serviceName,
          endpointId,
          dnsName,
          scopedConfigurationId,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL
            .foreignKey<UUID>(workspaceId)
            .references(WORKSPACE_TABLE, "id")
            .onDeleteCascade(),
          DSL
            .foreignKey<UUID>(dataplaneGroupId)
            .references("dataplane_group", "id"),
          DSL
            .foreignKey<UUID>(scopedConfigurationId)
            .references("scoped_configuration", "id"),
          DSL.unique(workspaceId, name),
        ).execute()

      ctx
        .createIndexIfNotExists("idx_private_link_workspace_id")
        .on(PRIVATE_LINK_TABLE, "workspace_id")
        .execute()
    }

    private fun createDataplaneNetworkConfigTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val dataplaneGroupId = DSL.field("dataplane_group_id", SQLDataType.UUID.nullable(false))
      val provider =
        DSL.field(
          "provider",
          SQLDataType.VARCHAR
            .asEnumDataType(CloudProvider::class.java)
            .nullable(false),
        )
      val config = DSL.field("config", DefaultDataType(null, String::class.java, "jsonb").nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(DATAPLANE_NETWORK_CONFIG_TABLE)
        .columns(
          id,
          dataplaneGroupId,
          provider,
          config,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.unique(dataplaneGroupId),
          DSL
            .foreignKey<UUID>(dataplaneGroupId)
            .references("dataplane_group", "id"),
        ).execute()
    }
  }
}
