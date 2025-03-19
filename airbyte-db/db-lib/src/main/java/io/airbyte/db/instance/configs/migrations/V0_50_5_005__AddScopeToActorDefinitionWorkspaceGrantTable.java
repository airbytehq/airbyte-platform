/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.unique;

import io.airbyte.config.ScopeType;
import java.util.ArrayList;
import java.util.List;
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
 * This migration adds scope_id and scope_type columns to the actor_definition_workspace_grant
 * table. The scope_type is an enum of either organization or workspace. The scope_id refers to the
 * id of the scope e.g. workspace_id or organization_id.
 */
public class V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_50_5_005__AddScopeToActorDefinitionWorkspaceGrantTable.class);
  private static final String ACTOR_DEFINITION_WORKSPACE_GRANT = "actor_definition_workspace_grant";
  private static final String SCOPE_TYPE = "scope_type";
  private static final Field<ScopeTypeEnum> NEW_SCOPE_TYPE_COLUMN = DSL.field(SCOPE_TYPE,
      SQLDataType.VARCHAR.asEnumDataType(ScopeTypeEnum.class).nullable(false).defaultValue(ScopeTypeEnum.workspace));

  private static final Field<UUID> WORKSPACE_ID_COLUMN = DSL.field("workspace_id", SQLDataType.UUID);
  private static final Field<UUID> ACTOR_DEFINITION_ID_COLUMN = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false));
  private static final Field<UUID> SCOPE_ID_COLUMN = DSL.field("scope_id", SQLDataType.UUID.nullable(true));

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    addScopeTypeEnum(ctx);
    addScopeColumn(ctx);
    alterTable(ctx);
    migrateExistingRows(ctx);
  }

  static void addScopeTypeEnum(final DSLContext ctx) {
    ctx.dropTypeIfExists(SCOPE_TYPE).execute();
    ctx.createType(SCOPE_TYPE).asEnum(ScopeType.WORKSPACE.value(), ScopeType.ORGANIZATION.value()).execute();
  }

  static void addScopeColumn(final DSLContext ctx) {
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).addColumnIfNotExists(SCOPE_ID_COLUMN).execute();
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).addColumnIfNotExists(NEW_SCOPE_TYPE_COLUMN).execute();

    LOGGER.info("scope_id and scope_type columns added to actor_definition_workspace_grant table");
  }

  /**
   * Add a foreign key constraint to the scope_id column and edit the primary key to be the scope_id,
   * actor_definition_id, and scope_type.
   */
  static void alterTable(final DSLContext ctx) {
    // make workspace_id column nullable
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).alterColumn(WORKSPACE_ID_COLUMN).dropNotNull().execute();

    // drop workspace foreign key
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).dropConstraintIfExists("actor_definition_workspace_grant_workspace_id_fkey")
        .execute();

    // drop unique constraint
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).dropConstraintIfExists("actor_definition_workspace_gr_workspace_id_actor_definition_key")
        .execute();

    // re-add unique constraint
    ctx.alterTable(ACTOR_DEFINITION_WORKSPACE_GRANT).add(unique(ACTOR_DEFINITION_ID_COLUMN, SCOPE_ID_COLUMN, NEW_SCOPE_TYPE_COLUMN)).execute();

    LOGGER.info("actor_definition_workspace_grant table altered");
  }

  /**
   * Migrate the existing table so that all the current rows have the scope_id and scope_type set to
   * the workspace_id and workspace respectively.
   */
  private static void migrateExistingRows(final DSLContext ctx) {
    final List<List<UUID>> actorDefinitionIdToWorkspaceIdList = new ArrayList<>();
    ctx.select(ACTOR_DEFINITION_ID_COLUMN, WORKSPACE_ID_COLUMN)
        .from(ACTOR_DEFINITION_WORKSPACE_GRANT)
        .stream()
        .forEach(record -> actorDefinitionIdToWorkspaceIdList.add(
            List.of(record.getValue(ACTOR_DEFINITION_ID_COLUMN), record.getValue(WORKSPACE_ID_COLUMN))));

    for (final List<UUID> actorDefinitionWorkspaceIdPair : actorDefinitionIdToWorkspaceIdList) {
      final UUID actorDefinitionId = actorDefinitionWorkspaceIdPair.get(0);
      final UUID workspaceId = actorDefinitionWorkspaceIdPair.get(1);
      ctx.update(DSL.table(ACTOR_DEFINITION_WORKSPACE_GRANT))
          .set(SCOPE_ID_COLUMN, workspaceId)
          .set(NEW_SCOPE_TYPE_COLUMN, ScopeTypeEnum.workspace)
          .where(ACTOR_DEFINITION_ID_COLUMN.eq(actorDefinitionId).and(WORKSPACE_ID_COLUMN.eq(workspaceId)))
          .execute();
    }

    LOGGER.info("Existing rows migrated");
  }

  enum ScopeTypeEnum implements EnumType {

    workspace(ScopeType.WORKSPACE.value()),
    organization(ScopeType.ORGANIZATION.value());

    private final String literal;

    ScopeTypeEnum(final String literal) {
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
      return SCOPE_TYPE;
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
