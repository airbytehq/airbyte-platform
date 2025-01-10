/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.constraint;

import io.airbyte.config.ScopeType;
import java.util.List;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Catalog;
import org.jooq.DSLContext;
import org.jooq.EnumType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V1_1_0_001__ConstraintToPreventInvitationScopePermissionMismatch.class);
  // user invitation table
  private static final Table<Record> USER_INVITATION_TABLE = DSL.table("user_invitation");
  private static final String CONSTRAINT_NAME = "user_invitation_scope_permission_mismatch";
  private static final Field<PermissionType> PERMISSION_TYPE = DSL.field("permission_type", SQLDataType.VARCHAR.asEnumDataType(PermissionType.class));
  private static final Field<ScopeTypeEnum> SCOPE_TYPE = DSL.field("scope_type", SQLDataType.VARCHAR.asEnumDataType(ScopeTypeEnum.class));
  private static final List<PermissionType> ORGANIZATION_PERMISSION_TYPES = List.of(
      PermissionType.ORGANIZATION_ADMIN,
      PermissionType.ORGANIZATION_EDITOR,
      PermissionType.ORGANIZATION_RUNNER,
      PermissionType.ORGANIZATION_READER);
  private static final List<PermissionType> WORKSPACE_PERMISSION_TYPES = List.of(
      PermissionType.WORKSPACE_ADMIN,
      PermissionType.WORKSPACE_EDITOR,
      PermissionType.WORKSPACE_READER,
      PermissionType.WORKSPACE_RUNNER);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    final DSLContext ctx = DSL.using(context.getConnection());
    runMigration(ctx);
  }

  public static void runMigration(final DSLContext ctx) {
    deleteInvalidRows(ctx);
    dropConstraintIfExists(ctx);
    addConstraint(ctx);
  }

  private static void deleteInvalidRows(final DSLContext ctx) {
    ctx.deleteFrom(USER_INVITATION_TABLE)
        .where(SCOPE_TYPE.eq(ScopeTypeEnum.workspace).and(PERMISSION_TYPE.notIn(WORKSPACE_PERMISSION_TYPES))
            .or(
                SCOPE_TYPE.eq(ScopeTypeEnum.organization).and(PERMISSION_TYPE.notIn(ORGANIZATION_PERMISSION_TYPES))))
        .execute();

  }

  public static void dropConstraintIfExists(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE).dropConstraintIfExists(CONSTRAINT_NAME).execute();
  }

  private static void addConstraint(final DSLContext ctx) {
    ctx.alterTable(USER_INVITATION_TABLE)
        .add(constraint(CONSTRAINT_NAME).check(
            SCOPE_TYPE.eq(ScopeTypeEnum.workspace).and(PERMISSION_TYPE.in(WORKSPACE_PERMISSION_TYPES))
                .or(
                    SCOPE_TYPE.eq(ScopeTypeEnum.organization).and(PERMISSION_TYPE.in(ORGANIZATION_PERMISSION_TYPES)))))
        .execute();
  }

  /**
   * User Roles as PermissionType enums.
   */
  enum PermissionType implements EnumType {

    INSTANCE_ADMIN("instance_admin"),
    ORGANIZATION_ADMIN("organization_admin"),
    ORGANIZATION_EDITOR("organization_editor"),
    ORGANIZATION_RUNNER("organization_runner"),
    ORGANIZATION_READER("organization_reader"),
    WORKSPACE_ADMIN("workspace_admin"),
    WORKSPACE_EDITOR("workspace_editor"),
    WORKSPACE_RUNNER("workspace_runner"),
    WORKSPACE_READER("workspace_reader");

    private final String literal;
    public static final String NAME = "permission_type";

    PermissionType(final String literal) {
      this.literal = literal;
    }

    @Override
    public @Nullable Catalog getCatalog() {
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
    public @NotNull String getLiteral() {
      return literal;
    }

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
      return "scope_type";
    }

    @Override
    public String getLiteral() {
      return literal;
    }

  }

}
