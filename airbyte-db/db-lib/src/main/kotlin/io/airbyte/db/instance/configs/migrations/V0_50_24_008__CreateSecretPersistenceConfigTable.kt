/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.ResourceScope
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

/**
 * This migration is being modified after the fact to account for an issue with certain Enterprise
 * installations that populated their database with a conflicting Keycloak data type of the same
 * name, `resource_scope`. This data type is not used by Airbyte, and was dropped in migration
 * V0_50_33_002__DropResourceScopeEnum. Enterprise installations that installed with this
 * V0_50_24_008 migration present, but without the V0_50_33_002 migration present, will need to
 * upgrade to the modified versions of these two migrations to proceed.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_008__CreateSecretPersistenceConfigTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addScopeTypeEnum(ctx)
    createTable(ctx)
  }

  internal enum class ResourceScope(
    private val literal: String,
  ) : EnumType {
    WORKSPACE(
      io.airbyte.config.ResourceScope.WORKSPACE
        .value(),
    ),
    ORGANIZATION(
      io.airbyte.config.ResourceScope.ORGANIZATION
        .value(),
    ),
    GLOBAL(
      io.airbyte.config.ResourceScope.GLOBAL
        .value(),
    ),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "resource_scope"

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val SECRET_PERSISTENCE_CONFIG = "secret_persistence_config"

    // the commented code below was present in the original version of this migration.
    // private static final String RESOURCE_SCOPE = "resource_scope";
    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false))

    // the commented code below was present in the original version of this migration.
    // private static final Field<ResourceScope> SCOPE_TYPE_COLUMN = DSL.field("scope_type",
    // SQLDataType.VARCHAR.asEnumDataType(ResourceScope.class).nullable(false).defaultValue(ResourceScope.GLOBAL));
    private val SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true))
    private val SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN = DSL.field("secret_persistence_config_coordinate", SQLDataType.VARCHAR(256))

    fun addScopeTypeEnum(ctx: DSLContext?) {
      // the commented code below was present in the original version of this migration.

      // ctx.dropTypeIfExists(RESOURCE_SCOPE).execute();
      // ctx.createType(RESOURCE_SCOPE).asEnum(
      // ResourceScope.WORKSPACE.getLiteral(),
      // ResourceScope.ORGANIZATION.getLiteral(),
      // ResourceScope.GLOBAL.getLiteral()).execute();
    }

    fun createTable(ctx: DSLContext) {
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(SECRET_PERSISTENCE_CONFIG)
        .columns(
          ID_COLUMN,
          SCOPE_ID_COLUMN, // the commented code below was present in the original version of this migration.
          // SCOPE_TYPE_COLUMN,
          SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN,
          createdAt,
          updatedAt,
        ) // the commented code below was present in the original version of this migration.
        .constraints(
          DSL.unique(
            SCOPE_ID_COLUMN, // SCOPE_TYPE_COLUMN ,
            SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN,
          ),
        ).primaryKey(ID_COLUMN)
        .execute()

      log.info { "$SECRET_PERSISTENCE_CONFIG table created" }
    }
  }
}
