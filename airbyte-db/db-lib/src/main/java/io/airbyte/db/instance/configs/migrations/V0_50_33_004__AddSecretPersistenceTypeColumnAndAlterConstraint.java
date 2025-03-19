/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.unique;

import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import java.time.OffsetDateTime;
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

public class V0_50_33_004__AddSecretPersistenceTypeColumnAndAlterConstraint extends BaseJavaMigration {

  private static final String SECRET_PERSISTENCE_CONFIG = "secret_persistence_config";
  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_004__AddSecretPersistenceTypeColumnAndAlterConstraint.class);

  private static final String SECRET_PERSISTENCE_TYPE = "secret_persistence_type";
  private static final String SECRET_PERSISTENCE_SCOPE_TYPE = "secret_persistence_scope_type";
  private static final String SCOPE_TYPE = "scope_type";

  private static final Field<SecretPersistenceScopeTypeEnum> SCOPE_TYPE_COLUMN = DSL.field(SCOPE_TYPE,
      SQLDataType.VARCHAR.asEnumDataType(SecretPersistenceScopeTypeEnum.class).nullable(false)
          .defaultValue(SecretPersistenceScopeTypeEnum.organization));

  private static final Field<UUID> SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true));
  private static final Field<SecretPersistenceType> SECRET_PERSISTENCE_TYPE_COLUMN = DSL.field(SECRET_PERSISTENCE_TYPE,
      SQLDataType.VARCHAR.asEnumDataType(SecretPersistenceType.class).nullable(false));

  private static final Field<String> SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN = DSL.field("secret_persistence_config_coordinate",
      SQLDataType.VARCHAR(256));

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    dropOriginalTable(ctx);
    addScopeType(ctx);
    addPersistenceType(ctx);
    createTable(ctx);
  }

  private void dropOriginalTable(final DSLContext ctx) {
    ctx.dropTableIfExists(SECRET_PERSISTENCE_CONFIG).execute();
  }

  private void createTable(final DSLContext ctx) {
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SECRET_PERSISTENCE_CONFIG)
        .columns(
            ID_COLUMN,
            SCOPE_ID_COLUMN,
            SCOPE_TYPE_COLUMN,
            SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN,
            SECRET_PERSISTENCE_TYPE_COLUMN,
            createdAt,
            updatedAt)
        .constraints(unique(SCOPE_ID_COLUMN, SCOPE_TYPE_COLUMN))
        .primaryKey(ID_COLUMN)
        .execute();

    LOGGER.info(SECRET_PERSISTENCE_CONFIG + " table created");
  }

  static void addPersistenceType(final DSLContext ctx) {
    ctx.dropTypeIfExists(SECRET_PERSISTENCE_TYPE).execute();
    ctx.createType(SECRET_PERSISTENCE_TYPE).asEnum(
        SecretPersistenceType.GOOGLE.getLiteral(),
        SecretPersistenceType.AWS.getLiteral(),
        SecretPersistenceType.VAULT.getLiteral(),
        SecretPersistenceType.TESTING.getLiteral()).execute();
  }

  static void addScopeType(final DSLContext ctx) {
    ctx.dropTypeIfExists(SECRET_PERSISTENCE_SCOPE_TYPE).execute();
    ctx.createType(SECRET_PERSISTENCE_SCOPE_TYPE).asEnum(
        SecretPersistenceScopeTypeEnum.workspace.getLiteral(),
        SecretPersistenceScopeTypeEnum.organization.getLiteral())
        .execute();
  }

  enum SecretPersistenceType implements EnumType {

    TESTING(SecretPersistenceConfig.SecretPersistenceType.TESTING.value()),
    GOOGLE(SecretPersistenceConfig.SecretPersistenceType.GOOGLE.value()),
    VAULT(SecretPersistenceConfig.SecretPersistenceType.VAULT.value()),
    AWS(SecretPersistenceConfig.SecretPersistenceType.AWS.value());

    private final String literal;

    SecretPersistenceType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return SECRET_PERSISTENCE_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  enum SecretPersistenceScopeTypeEnum implements EnumType {

    workspace(ScopeType.WORKSPACE.value()),
    organization(ScopeType.ORGANIZATION.value());

    private final String literal;

    SecretPersistenceScopeTypeEnum(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return SECRET_PERSISTENCE_SCOPE_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
