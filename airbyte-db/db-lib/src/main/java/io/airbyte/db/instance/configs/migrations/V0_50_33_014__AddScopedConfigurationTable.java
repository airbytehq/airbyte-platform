/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.primaryKey;

import java.sql.Date;
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

public class V0_50_33_014__AddScopedConfigurationTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_33_014__AddScopedConfigurationTable.class);

  private static final String CONFIG_SCOPE_TYPE = "config_scope_type";
  private static final String CONFIG_RESOURCE_TYPE = "config_resource_type";
  private static final String CONFIG_ORIGIN_TYPE = "config_origin_type";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createResourceTypeEnum(ctx);
    createScopeTypeEnum(ctx);
    createOriginTypeEnum(ctx);
    createScopedConfigurationTable(ctx);
  }

  static void createResourceTypeEnum(final DSLContext ctx) {
    ctx.createType(CONFIG_RESOURCE_TYPE).asEnum(ConfigResourceType.ACTOR_DEFINITION.literal).execute();
  }

  static void createScopeTypeEnum(final DSLContext ctx) {
    ctx.createType(CONFIG_SCOPE_TYPE).asEnum(ConfigScopeType.ORGANIZATION.literal, ConfigScopeType.WORKSPACE.literal, ConfigScopeType.ACTOR.literal)
        .execute();
  }

  static void createOriginTypeEnum(final DSLContext ctx) {
    ctx.createType(CONFIG_ORIGIN_TYPE).asEnum(ConfigOriginType.USER.literal).execute();
  }

  static void createScopedConfigurationTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<String> key = DSL.field("key", SQLDataType.VARCHAR(256).nullable(false));
    final Field<ConfigResourceType> resourceType =
        DSL.field("resource_type", SQLDataType.VARCHAR.asEnumDataType(ConfigResourceType.class).nullable(false));
    final Field<UUID> resourceId = DSL.field("resource_id", SQLDataType.UUID.nullable(false));
    final Field<ConfigScopeType> scopeType = DSL.field("scope_type", SQLDataType.VARCHAR.asEnumDataType(ConfigScopeType.class).nullable(false));
    final Field<UUID> scopeId = DSL.field("scope_id", SQLDataType.UUID.nullable(false));
    final Field<String> value = DSL.field("value", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> description = DSL.field("description", SQLDataType.CLOB.nullable(true));
    final Field<String> referenceUrl = DSL.field("reference_url", SQLDataType.VARCHAR(256).nullable(true));
    final Field<ConfigOriginType> originType = DSL.field("origin_type", SQLDataType.VARCHAR.asEnumDataType(ConfigOriginType.class).nullable(false));
    final Field<String> origin = DSL.field("origin", SQLDataType.VARCHAR(256).nullable(false));
    final Field<Date> expiresAt = DSL.field("expires_at", SQLDataType.DATE.nullable(true));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));

    ctx.createTableIfNotExists("scoped_configuration")
        .columns(id,
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
            updatedAt)
        .constraints(primaryKey(id))
        .unique(key, resourceType, resourceId, scopeType, scopeId)
        .execute();
  }

  enum ConfigResourceType implements EnumType {

    ACTOR_DEFINITION("actor_definition");

    private final String literal;

    ConfigResourceType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return CONFIG_RESOURCE_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  enum ConfigScopeType implements EnumType {

    ORGANIZATION("organization"),
    WORKSPACE("workspace"),
    ACTOR("actor");

    private final String literal;

    ConfigScopeType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return CONFIG_SCOPE_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

  enum ConfigOriginType implements EnumType {

    USER("user");

    private final String literal;

    ConfigOriginType(final String literal) {
      this.literal = literal;
    }

    @Override
    public Catalog getCatalog() {
      return getSchema() == null ? null : getSchema().getCatalog();
    }

    @Override
    public Schema getSchema() {
      return new SchemaImpl(DSL.name("public"), null);
    }

    @Override
    public String getName() {
      return CONFIG_ORIGIN_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
