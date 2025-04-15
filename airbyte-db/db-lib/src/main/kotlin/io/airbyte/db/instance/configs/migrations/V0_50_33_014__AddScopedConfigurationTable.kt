/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_33_014__AddScopedConfigurationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createResourceTypeEnum(ctx)
    createScopeTypeEnum(ctx)
    createOriginTypeEnum(ctx)
    createScopedConfigurationTable(ctx)
  }

  enum class ConfigResourceType(
    private val literal: String,
  ) : EnumType {
    ACTOR_DEFINITION("actor_definition"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONFIG_RESOURCE_TYPE

    override fun getLiteral(): String = literal
  }

  enum class ConfigScopeType(
    private val literal: String,
  ) : EnumType {
    ORGANIZATION("organization"),
    WORKSPACE("workspace"),
    ACTOR("actor"),
    ;

    override fun getCatalog(): Catalog? = if (schema == null) null else schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONFIG_SCOPE_TYPE

    override fun getLiteral(): String = literal
  }

  enum class ConfigOriginType(
    private val literal: String,
  ) : EnumType {
    USER("user"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONFIG_ORIGIN_TYPE

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val CONFIG_SCOPE_TYPE = "config_scope_type"
    private const val CONFIG_RESOURCE_TYPE = "config_resource_type"
    private const val CONFIG_ORIGIN_TYPE = "config_origin_type"

    @JvmStatic
    fun createResourceTypeEnum(ctx: DSLContext) {
      ctx.createType(CONFIG_RESOURCE_TYPE).asEnum(ConfigResourceType.ACTOR_DEFINITION.literal).execute()
    }

    @JvmStatic
    fun createScopeTypeEnum(ctx: DSLContext) {
      ctx
        .createType(CONFIG_SCOPE_TYPE)
        .asEnum(
          ConfigScopeType.ORGANIZATION.literal,
          ConfigScopeType.WORKSPACE.literal,
          ConfigScopeType.ACTOR.literal,
        ).execute()
    }

    @JvmStatic
    fun createOriginTypeEnum(ctx: DSLContext) {
      ctx.createType(CONFIG_ORIGIN_TYPE).asEnum(ConfigOriginType.USER.literal).execute()
    }

    @JvmStatic
    fun createScopedConfigurationTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val key = DSL.field("key", SQLDataType.VARCHAR(256).nullable(false))
      val resourceType =
        DSL.field(
          "resource_type",
          SQLDataType.VARCHAR
            .asEnumDataType(
              ConfigResourceType::class.java,
            ).nullable(false),
        )
      val resourceId = DSL.field("resource_id", SQLDataType.UUID.nullable(false))
      val scopeType =
        DSL.field(
          "scope_type",
          SQLDataType.VARCHAR
            .asEnumDataType(
              ConfigScopeType::class.java,
            ).nullable(false),
        )
      val scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false))
      val value = DSL.field("value", SQLDataType.VARCHAR(256).nullable(false))
      val description = DSL.field("description", SQLDataType.CLOB.nullable(true))
      val referenceUrl = DSL.field("reference_url", SQLDataType.VARCHAR(256).nullable(true))
      val originType =
        DSL.field(
          "origin_type",
          SQLDataType.VARCHAR
            .asEnumDataType(
              ConfigOriginType::class.java,
            ).nullable(false),
        )
      val origin = DSL.field("origin", SQLDataType.VARCHAR(256).nullable(false))
      val expiresAt = DSL.field("expires_at", SQLDataType.DATE.nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("scoped_configuration")
        .columns(
          id,
          key,
          resourceType,
          resourceId,
          scopeType,
          scopeId,
          value,
          description,
          referenceUrl,
          originType,
          origin,
          expiresAt,
          createdAt,
          updatedAt,
        ).constraints(DSL.primaryKey(id))
        .unique(key, resourceType, resourceId, scopeType, scopeId)
        .execute()
    }
  }
}
