/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.unique;

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

/**
 * This migration is being modified after the fact to account for an issue with certain Enterprise
 * installations that populated their database with a conflicting Keycloak data type of the same
 * name, `resource_scope`. This data type is not used by Airbyte, and was dropped in migration
 * V0_50_33_002__DropResourceScopeEnum. Enterprise installations that installed with this
 * V0_50_24_008 migration present, but without the V0_50_33_002 migration present, will need to
 * upgrade to the modified versions of these two migrations to proceed.
 */
public class V0_50_24_008__CreateSecretPersistenceConfigTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_24_008__CreateSecretPersistenceConfigTable.class);
  private static final String SECRET_PERSISTENCE_CONFIG = "secret_persistence_config";

  // the commented code below was present in the original version of this migration.
  // private static final String RESOURCE_SCOPE = "resource_scope";

  private static final Field<UUID> ID_COLUMN = DSL.field("id", SQLDataType.UUID.nullable(false));

  // the commented code below was present in the original version of this migration.
  // private static final Field<ResourceScope> SCOPE_TYPE_COLUMN = DSL.field("scope_type",
  // SQLDataType.VARCHAR.asEnumDataType(ResourceScope.class).nullable(false).defaultValue(ResourceScope.GLOBAL));

  private static final Field<UUID> SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true));
  private static final Field<String> SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN = DSL.field("secret_persistence_config_coordinate",
      SQLDataType.VARCHAR(256));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addScopeTypeEnum(ctx);
    createTable(ctx);
  }

  static void addScopeTypeEnum(final DSLContext ctx) {
    // the commented code below was present in the original version of this migration.

    // ctx.dropTypeIfExists(RESOURCE_SCOPE).execute();
    // ctx.createType(RESOURCE_SCOPE).asEnum(
    // ResourceScope.WORKSPACE.getLiteral(),
    // ResourceScope.ORGANIZATION.getLiteral(),
    // ResourceScope.GLOBAL.getLiteral()).execute();
  }

  static void createTable(final DSLContext ctx) {
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists(SECRET_PERSISTENCE_CONFIG)
        .columns(
            ID_COLUMN,
            SCOPE_ID_COLUMN,
            // the commented code below was present in the original version of this migration.
            // SCOPE_TYPE_COLUMN,
            SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN,
            createdAt,
            updatedAt)
        // the commented code below was present in the original version of this migration.
        .constraints(unique(SCOPE_ID_COLUMN, /* SCOPE_TYPE_COLUMN , */ SECRET_PERSISTENCE_CONFIG_COORDINATE_COLUMN))
        .primaryKey(ID_COLUMN)
        .execute();

    LOGGER.info(SECRET_PERSISTENCE_CONFIG + " table created");
  }

  enum ResourceScope implements EnumType {

    WORKSPACE(io.airbyte.config.ResourceScope.WORKSPACE.value()),
    ORGANIZATION(io.airbyte.config.ResourceScope.ORGANIZATION.value()),
    GLOBAL(io.airbyte.config.ResourceScope.GLOBAL.value());

    private final String literal;

    ResourceScope(final String literal) {
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
      return "resource_scope";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
