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

/**
 * Adds SecretConfig, SecretReference, and SecretConfigReference tables to the Configs database.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_1_011__AddSecretConfigStorageAndReferenceTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createSecretStorageTypeEnum(ctx)
    createSecretStorageScopeTypeEnum(ctx)
    createSecretReferenceScopeTypeEnum(ctx)

    createSecretStorageTable(ctx)
    createSecretConfigTable(ctx)
    createSecretReferenceTable(ctx)
  }

  /**
   * new SecretStorageType data type enum.
   */
  enum class SecretStorageType(
    private val literal: String,
  ) : EnumType {
    AWS_SECRETS_MANAGER("aws_secrets_manager"),
    GOOGLE_SECRET_MANAGER("google_secret_manager"),
    AZURE_KEY_VAULT("azure_key_vault"),
    VAULT("vault"),
    LOCAL_TESTING("local_testing"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "secret_storage_type"
    }
  }

  /**
   * new SecretStorageScopeType data type enum.
   */
  enum class SecretStorageScopeType(
    private val literal: String,
  ) : EnumType {
    WORKSPACE("workspace"),
    ORGANIZATION("organization"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "secret_storage_scope_type"
    }
  }

  /**
   * new SecretReferenceScopeType data type enum.
   */
  enum class SecretReferenceScopeType(
    private val literal: String,
  ) : EnumType {
    ACTOR("actor"),
    SECRET_STORAGE("secret_storage"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"))

    override fun getName(): String? = NAME

    override fun getLiteral(): String = literal

    companion object {
      const val NAME: String = "secret_reference_scope_type"
    }
  }

  companion object {
    private const val SECRET_CONFIG_TABLE_NAME = "secret_config"
    private const val SECRET_STORAGE_TABLE_NAME = "secret_storage"
    private const val SECRET_REFERENCE_TABLE_NAME = "secret_reference"

    fun createSecretStorageTypeEnum(ctx: DSLContext) {
      ctx
        .createType(SecretStorageType.NAME)
        .asEnum(*SecretStorageType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createSecretStorageScopeTypeEnum(ctx: DSLContext) {
      ctx
        .createType(SecretStorageScopeType.NAME)
        .asEnum(*SecretStorageScopeType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    fun createSecretReferenceScopeTypeEnum(ctx: DSLContext) {
      ctx
        .createType(SecretReferenceScopeType.NAME)
        .asEnum(*SecretReferenceScopeType.entries.map { it.literal }.toTypedArray())
        .execute()
    }

    private fun createSecretStorageTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val scopeType =
        DSL.field(
          "scope_type",
          SQLDataType.VARCHAR.asEnumDataType(SecretStorageScopeType::class.java).nullable(false),
        )
      val scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false))
      val descriptor = DSL.field("descriptor", SQLDataType.VARCHAR(256).nullable(false))
      val storageType =
        DSL.field(
          "storage_type",
          SQLDataType.VARCHAR.asEnumDataType(SecretStorageType::class.java).nullable(false),
        )
      val configuredFromEnvironment = DSL.field("configured_from_environment", SQLDataType.BOOLEAN.defaultValue(false).nullable(false))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.defaultValue(false).nullable(false))
      val createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false))
      val updatedBy = DSL.field("updated_by", SQLDataType.UUID.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(SECRET_STORAGE_TABLE_NAME)
        .columns(
          id,
          scopeType,
          scopeId,
          descriptor,
          storageType,
          configuredFromEnvironment,
          tombstone,
          createdBy,
          updatedBy,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.unique(scopeId, scopeType, storageType, descriptor),
        ).execute()
    }

    private fun createSecretConfigTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val secretStorageId = DSL.field("secret_storage_id", SQLDataType.UUID.nullable(false))
      val descriptor = DSL.field("descriptor", SQLDataType.VARCHAR(256).nullable(false))
      val externalCoordinate = DSL.field("external_coordinate", SQLDataType.VARCHAR(256).nullable(false))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.defaultValue(false).nullable(false))
      val createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false))
      val updatedBy = DSL.field("updated_by", SQLDataType.UUID.nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(SECRET_CONFIG_TABLE_NAME)
        .columns(
          id,
          secretStorageId,
          descriptor,
          externalCoordinate,
          tombstone,
          createdBy,
          updatedBy,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(secretStorageId).references(SECRET_STORAGE_TABLE_NAME, "id"),
          DSL.unique(secretStorageId, descriptor),
          DSL.unique(secretStorageId, externalCoordinate),
        ).execute()

      ctx
        .alterTable(SECRET_CONFIG_TABLE_NAME)
        .add(
          DSL
            .foreignKey(DSL.field("secret_storage_id", SQLDataType.UUID))
            .references(SECRET_STORAGE_TABLE_NAME, "id"),
        ).execute()
    }

    private fun createSecretReferenceTable(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val secretConfigId = DSL.field("secret_config_id", SQLDataType.UUID.nullable(false))
      val scopeType =
        DSL.field(
          "scope_type",
          SQLDataType.VARCHAR.asEnumDataType(SecretReferenceScopeType::class.java).nullable(false),
        )
      val scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false))
      val hydrationPath = DSL.field("hydration_path", SQLDataType.CLOB.nullable(true)) // CLOB becomes the TEXT type in Postgres.
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists(SECRET_REFERENCE_TABLE_NAME)
        .columns(
          id,
          secretConfigId,
          scopeType,
          scopeId,
          hydrationPath,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(secretConfigId).references(SECRET_CONFIG_TABLE_NAME, "id"),
        ).execute()

      // create a unique index on scopeType, scopeId, and hydrationPath to ensure that there is only one
      // secret_reference for a given scope and path.
      // Coalesce null hydrationPaths to empty strings to ensure uniqueness, because Postgres allows
      // multiple null values in a unique index by default.
      ctx
        .createUniqueIndexIfNotExists("secret_reference_scope_type_scope_id_hydration_path_idx")
        .on(
          DSL.table(SECRET_REFERENCE_TABLE_NAME),
          DSL.field("scope_type"),
          DSL.field("scope_id"),
          DSL.field("coalesce(hydration_path, '')"),
        ).execute()

      // create index on secretConfigId to efficiently query all secret_references for a given
      // secret_config.
      ctx
        .createIndexIfNotExists("secret_reference_secret_config_id_idx")
        .on(SECRET_REFERENCE_TABLE_NAME, secretConfigId.name)
        .execute()
    }
  }
}
