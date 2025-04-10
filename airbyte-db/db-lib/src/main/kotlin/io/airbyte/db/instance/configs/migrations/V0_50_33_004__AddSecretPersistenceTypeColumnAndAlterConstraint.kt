/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
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
class V0_50_33_004__AddSecretPersistenceTypeColumnAndAlterConstraint : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropOriginalTable(ctx)
    addScopeType(ctx)
    addPersistenceType(ctx)
    createTable(ctx)
  }

  private fun dropOriginalTable(ctx: DSLContext) {
    ctx.dropTableIfExists(SECRET_PERSISTENCE_CONFIG).execute()
  }

  private fun createTable(ctx: DSLContext) {
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

    ctx
      .createTableIfNotExists(SECRET_PERSISTENCE_CONFIG)
      .columns(
        ID_COLUMN,
        SCOPE_ID_COLUMN,
        SCOPE_TYPE_COLUMN,
        SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN,
        SECRET_PERSISTENCE_TYPE_COLUMN,
        createdAt,
        updatedAt,
      ).constraints(DSL.unique(SCOPE_ID_COLUMN, SCOPE_TYPE_COLUMN))
      .primaryKey(ID_COLUMN)
      .execute()

    log.info { "$SECRET_PERSISTENCE_CONFIG table created" }
  }

  internal enum class SecretPersistenceType(
    private val literal: String,
  ) : EnumType {
    TESTING(SecretPersistenceConfig.SecretPersistenceType.TESTING.value()),
    GOOGLE(SecretPersistenceConfig.SecretPersistenceType.GOOGLE.value()),
    VAULT(SecretPersistenceConfig.SecretPersistenceType.VAULT.value()),
    AWS(SecretPersistenceConfig.SecretPersistenceType.AWS.value()),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = SECRET_PERSISTENCE_TYPE

    override fun getLiteral(): String = literal
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  internal enum class SecretPersistenceScopeTypeEnum(
    private val literal: String,
  ) : EnumType {
    workspace(ScopeType.WORKSPACE.value()),
    organization(ScopeType.ORGANIZATION.value()),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = SECRET_PERSISTENCE_SCOPE_TYPE

    override fun getLiteral(): String = literal
  }

  companion object {
    private const val SECRET_PERSISTENCE_CONFIG = "secret_persistence_config"
    private const val SECRET_PERSISTENCE_TYPE = "secret_persistence_type"
    private const val SECRET_PERSISTENCE_SCOPE_TYPE = "secret_persistence_scope_type"
    private const val SCOPE_TYPE = "scope_type"

    private val SCOPE_TYPE_COLUMN =
      DSL.field(
        SCOPE_TYPE,
        SQLDataType.VARCHAR
          .asEnumDataType(SecretPersistenceScopeTypeEnum::class.java)
          .nullable(false)
          .defaultValue(SecretPersistenceScopeTypeEnum.organization),
      )

    private val SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true))
    private val SECRET_PERSISTENCE_TYPE_COLUMN =
      DSL.field(
        SECRET_PERSISTENCE_TYPE,
        SQLDataType.VARCHAR
          .asEnumDataType(
            SecretPersistenceType::class.java,
          ).nullable(false),
      )

    private val SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN =
      DSL.field(
        "secret_persistence_config_coordinate",
        SQLDataType.VARCHAR(256),
      )

    private val ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false))

    fun addPersistenceType(ctx: DSLContext) {
      ctx.dropTypeIfExists(SECRET_PERSISTENCE_TYPE).execute()
      ctx
        .createType(SECRET_PERSISTENCE_TYPE)
        .asEnum(
          SecretPersistenceType.GOOGLE.literal,
          SecretPersistenceType.AWS.literal,
          SecretPersistenceType.VAULT.literal,
          SecretPersistenceType.TESTING.literal,
        ).execute()
    }

    fun addScopeType(ctx: DSLContext) {
      ctx.dropTypeIfExists(SECRET_PERSISTENCE_SCOPE_TYPE).execute()
      ctx
        .createType(SECRET_PERSISTENCE_SCOPE_TYPE)
        .asEnum(
          SecretPersistenceScopeTypeEnum.workspace.literal,
          SecretPersistenceScopeTypeEnum.organization.literal,
        ).execute()
    }
  }
}
