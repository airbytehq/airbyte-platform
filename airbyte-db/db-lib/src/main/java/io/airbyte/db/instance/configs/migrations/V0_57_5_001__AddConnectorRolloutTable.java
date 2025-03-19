/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jetbrains.annotations.NotNull;
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

public class V0_57_5_001__AddConnectorRolloutTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_5_001__AddConnectorRolloutTable.class);
  private static final Table<Record> CONNECTOR_ROLLOUT_TABLE = DSL.table("connector_rollout");
  private static final Field<UUID> ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", UUID.class);
  private static final Field<String> STATE = DSL.field("state", String.class);
  private static final String CONNECTOR_ROLLOUT_STATE_CATEGORY = "connector_rollout_state_category";
  private static final String CONNECTOR_ROLLOUT_STATE_TYPE = "connector_rollout_state_type";
  private static final String CONNECTOR_ROLLOUT_STRATEGY_TYPE = "connector_rollout_strategy_type";

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    createStateCategoryEnum(ctx);
    createStateTypeEnum(ctx);
    createStrategyTypeEnum(ctx);
    createRolloutTable(ctx);
    createPartialUniqueIndex(ctx);
    LOGGER.info("connector_rollout table created");
  }

  static void createStateTypeEnum(final DSLContext ctx) {
    ctx.createType(CONNECTOR_ROLLOUT_STATE_TYPE).asEnum(
        ConnectorRolloutStateType.INITIALIZED.literal,
        ConnectorRolloutStateType.IN_PROGRESS.literal,
        ConnectorRolloutStateType.PAUSED.literal,
        ConnectorRolloutStateType.FINALIZING.literal,
        ConnectorRolloutStateType.SUCCEEDED.literal,
        ConnectorRolloutStateType.ERRORED.literal,
        ConnectorRolloutStateType.FAILED_ROLLED_BACK.literal,
        ConnectorRolloutStateType.CANCELED_ROLLED_BACK.literal).execute();
  }

  static void createStateCategoryEnum(final DSLContext ctx) {
    ctx.createType(CONNECTOR_ROLLOUT_STATE_CATEGORY).asEnum(
        ConnectorRolloutStateCategory.ACTIVE.literal,
        ConnectorRolloutStateCategory.TERMINAL.literal).execute();
  }

  static void createStrategyTypeEnum(final DSLContext ctx) {
    ctx.createType(CONNECTOR_ROLLOUT_STRATEGY_TYPE)
        .asEnum(ConnectorRolloutStrategyType.MANUAL.literal, ConnectorRolloutStrategyType.AUTOMATED.literal,
            ConnectorRolloutStrategyType.OVERRIDDEN.literal)
        .execute();
  }

  static void createRolloutTable(final DSLContext ctx) {
    ctx.createTableIfNotExists(CONNECTOR_ROLLOUT_TABLE)
        .column("id", SQLDataType.UUID.nullable(false))
        .column("actor_definition_id", SQLDataType.UUID.nullable(false))
        .column("release_candidate_version_id", SQLDataType.UUID.nullable(false))
        .column("initial_version_id", SQLDataType.UUID.nullable(true))
        .column("state", SQLDataType.VARCHAR(32).nullable(false))
        .column("initial_rollout_pct", SQLDataType.INTEGER.nullable(false))
        .column("current_target_rollout_pct", SQLDataType.INTEGER.nullable(true))
        .column("final_target_rollout_pct", SQLDataType.INTEGER.nullable(false))
        .column("has_breaking_changes", SQLDataType.BOOLEAN.nullable(false))
        .column("max_step_wait_time_mins", SQLDataType.INTEGER.nullable(false))
        .column("updated_by", SQLDataType.UUID.nullable(true))
        .column("created_at", SQLDataType.TIMESTAMP.nullable(false).defaultValue(DSL.currentTimestamp()))
        .column("updated_at", SQLDataType.TIMESTAMP.nullable(false).defaultValue(DSL.currentTimestamp()))
        .column("completed_at", SQLDataType.TIMESTAMP.nullable(true))
        .column("expires_at", SQLDataType.TIMESTAMP.nullable(false))
        .column("error_msg", SQLDataType.VARCHAR(1024).nullable(true))
        .column("failed_reason", SQLDataType.VARCHAR(1024).nullable(true))
        .column("rollout_strategy", SQLDataType.VARCHAR(256).nullable(false))
        .constraints(
            DSL.constraint("pk_connector_rollout").primaryKey("id"),
            DSL.constraint("fk_actor_definition_id").foreignKey("actor_definition_id").references("actor_definition", "id"),
            DSL.constraint("fk_initial_version_id").foreignKey("initial_version_id").references("actor_definition_version", "id"),
            DSL.constraint("fk_release_candidate_version_id").foreignKey("release_candidate_version_id").references("actor_definition_version", "id"),
            DSL.constraint("fk_updated_by").foreignKey("updated_by").references("user", "id"))
        .execute();
  }

  static void createPartialUniqueIndex(final DSLContext ctx) {
    // Create a partial unique index to guarantee that only one active rollout exists for a given
    // connector.
    ctx.createUniqueIndex("actor_definition_id_state_unique_idx")
        .on(CONNECTOR_ROLLOUT_TABLE, ACTOR_DEFINITION_ID)
        .where(STATE.in(ConnectorRolloutStateType.getActiveStates())).execute();
  }

  public enum ConnectorRolloutStateCategory implements EnumType {

    ACTIVE("active"),
    TERMINAL("terminal");

    private final String literal;

    ConnectorRolloutStateCategory(final String literal) {
      this.literal = literal;
    }

    @Override
    public @NotNull String getLiteral() {
      return literal;
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
      return CONNECTOR_ROLLOUT_STATE_CATEGORY;
    }

  }

  public enum ConnectorRolloutStateType implements EnumType {

    INITIALIZED("initialized", ConnectorRolloutStateCategory.ACTIVE),
    IN_PROGRESS("in_progress", ConnectorRolloutStateCategory.ACTIVE),
    PAUSED("paused", ConnectorRolloutStateCategory.ACTIVE),
    FINALIZING("finalizing", ConnectorRolloutStateCategory.ACTIVE),
    ERRORED("errored", ConnectorRolloutStateCategory.ACTIVE),
    SUCCEEDED("succeeded", ConnectorRolloutStateCategory.TERMINAL),
    FAILED_ROLLED_BACK("failed_rolled_back", ConnectorRolloutStateCategory.TERMINAL),
    CANCELED_ROLLED_BACK("canceled_rolled_back", ConnectorRolloutStateCategory.TERMINAL);

    private final String literal;
    private final ConnectorRolloutStateCategory category;

    ConnectorRolloutStateType(final String literal, final ConnectorRolloutStateCategory category) {
      this.literal = literal;
      this.category = category;
    }

    @Override
    public @NotNull String getLiteral() {
      return literal;
    }

    public ConnectorRolloutStateCategory getCategory() {
      return category;
    }

    public static Set<ConnectorRolloutStateType> getActiveStates() {
      return EnumSet.allOf(ConnectorRolloutStateType.class).stream()
          .filter(state -> state.getCategory() == ConnectorRolloutStateCategory.ACTIVE)
          .sorted((state1, state2) -> state1.getLiteral().compareTo(state2.getLiteral()))
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static Set<ConnectorRolloutStateType> getTerminalStates() {
      return EnumSet.allOf(ConnectorRolloutStateType.class).stream()
          .filter(state -> state.getCategory() == ConnectorRolloutStateCategory.TERMINAL)
          .collect(Collectors.toSet());
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
      return CONNECTOR_ROLLOUT_STATE_TYPE;
    }

  }

  enum ConnectorRolloutStrategyType implements EnumType {

    MANUAL("manual"),
    AUTOMATED("automated"),
    OVERRIDDEN("overridden");

    private final String literal;

    ConnectorRolloutStrategyType(final String literal) {
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
      return CONNECTOR_ROLLOUT_STRATEGY_TYPE;
    }

    @Override
    public @NotNull String getLiteral() {
      return literal;
    }

  }

}
