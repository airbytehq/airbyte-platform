/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;
import static org.jooq.impl.DSL.unique;

import java.time.OffsetDateTime;
import java.util.Arrays;
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
 * Adds SecretConfig, SecretReference, and SecretConfigReference tables to the Configs database.
 */
public class V1_1_1_011__AddSecretConfigStorageAndReferenceTables extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_1_011__AddSecretConfigStorageAndReferenceTables.class);

  private static final String SECRET_CONFIG_TABLE_NAME = "secret_config";
  private static final String SECRET_STORAGE_TABLE_NAME = "secret_storage";
  private static final String SECRET_REFERENCE_TABLE_NAME = "secret_reference";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createSecretStorageTypeEnum(ctx);
    createSecretStorageScopeTypeEnum(ctx);
    createSecretReferenceScopeTypeEnum(ctx);

    createSecretStorageTable(ctx);
    createSecretConfigTable(ctx);
    createSecretReferenceTable(ctx);
  }

  static void createSecretStorageTypeEnum(final DSLContext ctx) {
    ctx.createType(SecretStorageType.NAME)
        .asEnum(Arrays.stream(SecretStorageType.values()).map(SecretStorageType::getLiteral).toArray(String[]::new))
        .execute();
  }

  static void createSecretStorageScopeTypeEnum(final DSLContext ctx) {
    ctx.createType(SecretStorageScopeType.NAME)
        .asEnum(Arrays.stream(SecretStorageScopeType.values()).map(SecretStorageScopeType::getLiteral).toArray(String[]::new))
        .execute();
  }

  static void createSecretReferenceScopeTypeEnum(final DSLContext ctx) {
    ctx.createType(SecretReferenceScopeType.NAME)
        .asEnum(Arrays.stream(SecretReferenceScopeType.values()).map(SecretReferenceScopeType::getLiteral).toArray(String[]::new))
        .execute();
  }

  private static void createSecretStorageTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<SecretStorageScopeType> scopeType =
        DSL.field("scope_type", SQLDataType.VARCHAR.asEnumDataType(SecretStorageScopeType.class).nullable(false));
    final Field<UUID> scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false));
    final Field<String> descriptor = DSL.field("descriptor", SQLDataType.VARCHAR(256).nullable(false));
    final Field<SecretStorageType> storageType =
        DSL.field("storage_type", SQLDataType.VARCHAR.asEnumDataType(SecretStorageType.class).nullable(false));
    final Field<Boolean> configuredFromEnvironment =
        DSL.field("configured_from_environment", SQLDataType.BOOLEAN.defaultValue(false).nullable(false));
    final Field<Boolean> tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.defaultValue(false).nullable(false));
    final Field<UUID> createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false));
    final Field<UUID> updatedBy = DSL.field("updated_by", SQLDataType.UUID.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SECRET_STORAGE_TABLE_NAME)
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
            updatedAt)
        .constraints(
            primaryKey(id),
            unique(scopeId, scopeType, storageType, descriptor))
        .execute();
  }

  private static void createSecretConfigTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> secretStorageId = DSL.field("secret_storage_id", SQLDataType.UUID.nullable(false));
    final Field<String> descriptor = DSL.field("descriptor", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> externalCoordinate = DSL.field("external_coordinate", SQLDataType.VARCHAR(256).nullable(false));
    final Field<Boolean> tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.defaultValue(false).nullable(false));
    final Field<UUID> createdBy = DSL.field("created_by", SQLDataType.UUID.nullable(false));
    final Field<UUID> updatedBy = DSL.field("updated_by", SQLDataType.UUID.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SECRET_CONFIG_TABLE_NAME)
        .columns(
            id,
            secretStorageId,
            descriptor,
            externalCoordinate,
            tombstone,
            createdBy,
            updatedBy,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(secretStorageId).references(SECRET_STORAGE_TABLE_NAME, "id"),
            unique(secretStorageId, descriptor),
            unique(secretStorageId, externalCoordinate))
        .execute();

    ctx.alterTable(SECRET_CONFIG_TABLE_NAME)
        .add(foreignKey(DSL.field("secret_storage_id", SQLDataType.UUID))
            .references(SECRET_STORAGE_TABLE_NAME, "id"))
        .execute();
  }

  private static void createSecretReferenceTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> secretConfigId = DSL.field("secret_config_id", SQLDataType.UUID.nullable(false));
    final Field<SecretReferenceScopeType> scopeType =
        DSL.field("scope_type", SQLDataType.VARCHAR.asEnumDataType(SecretReferenceScopeType.class).nullable(false));
    final Field<UUID> scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false));
    final Field<String> hydrationPath = DSL.field("hydration_path", SQLDataType.CLOB.nullable(true)); // CLOB becomes the TEXT type in Postgres.
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SECRET_REFERENCE_TABLE_NAME)
        .columns(
            id,
            secretConfigId,
            scopeType,
            scopeId,
            hydrationPath,
            createdAt,
            updatedAt)
        .constraints(
            primaryKey(id),
            foreignKey(secretConfigId).references(SECRET_CONFIG_TABLE_NAME, "id"))
        .execute();

    // create a unique index on scopeType, scopeId, and hydrationPath to ensure that there is only one
    // secret_reference for a given scope and path.
    // Coalesce null hydrationPaths to empty strings to ensure uniqueness, because Postgres allows
    // multiple null values in a unique index by default.
    ctx.createUniqueIndexIfNotExists("secret_reference_scope_type_scope_id_hydration_path_idx")
        .on(DSL.table(SECRET_REFERENCE_TABLE_NAME), DSL.field("scope_type"), DSL.field("scope_id"), DSL.field("coalesce(hydration_path, '')"))
        .execute();

    // create index on secretConfigId to efficiently query all secret_references for a given
    // secret_config.
    ctx.createIndexIfNotExists("secret_reference_secret_config_id_idx")
        .on(SECRET_REFERENCE_TABLE_NAME, secretConfigId.getName())
        .execute();
  }

  /**
   * new SecretStorageType data type enum.
   */
  public enum SecretStorageType implements EnumType {

    AWS_SECRETS_MANAGER("aws_secrets_manager"),
    GOOGLE_SECRET_MANAGER("google_secret_manager"),
    AZURE_KEY_VAULT("azure_key_vault"),
    VAULT("vault"),
    LOCAL_TESTING("local_testing");

    private final String literal;
    public static final String NAME = "secret_storage_type";

    SecretStorageType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  /**
   * new SecretStorageScopeType data type enum.
   */
  public enum SecretStorageScopeType implements EnumType {

    WORKSPACE("workspace"),
    ORGANIZATION("organization");

    private final String literal;
    public static final String NAME = "secret_storage_scope_type";

    SecretStorageScopeType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  /**
   * new SecretReferenceScopeType data type enum.
   */
  public enum SecretReferenceScopeType implements EnumType {

    ACTOR("actor"),
    SECRET_STORAGE("secret_storage");

    private final String literal;
    public static final String NAME = "secret_reference_scope_type";

    SecretReferenceScopeType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
