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
import java.util.UUID

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_5_001__AddConnectorRolloutTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createStateCategoryEnum(ctx)
    createStateTypeEnum(ctx)
    createStrategyTypeEnum(ctx)
    createRolloutTable(ctx)
    createPartialUniqueIndex(ctx)
    log.info { "connector_rollout table created" }
  }

  enum class ConnectorRolloutStateCategory(
    private val literal: String,
  ) : EnumType {
    ACTIVE("active"),
    TERMINAL("terminal"),
    ;

    override fun getLiteral(): String = literal

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONNECTOR_ROLLOUT_STATE_CATEGORY
  }

  enum class ConnectorRolloutStateType(
    private val literal: String,
    val category: ConnectorRolloutStateCategory,
  ) : EnumType {
    INITIALIZED("initialized", ConnectorRolloutStateCategory.ACTIVE),
    IN_PROGRESS("in_progress", ConnectorRolloutStateCategory.ACTIVE),
    PAUSED("paused", ConnectorRolloutStateCategory.ACTIVE),
    FINALIZING("finalizing", ConnectorRolloutStateCategory.ACTIVE),
    ERRORED("errored", ConnectorRolloutStateCategory.ACTIVE),
    SUCCEEDED("succeeded", ConnectorRolloutStateCategory.TERMINAL),
    FAILED_ROLLED_BACK("failed_rolled_back", ConnectorRolloutStateCategory.TERMINAL),
    CANCELED_ROLLED_BACK("canceled_rolled_back", ConnectorRolloutStateCategory.TERMINAL),
    ;

    override fun getLiteral(): String = literal

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONNECTOR_ROLLOUT_STATE_TYPE

    companion object {
      val activeStates: Set<ConnectorRolloutStateType?>
        get() =
          ConnectorRolloutStateType.entries
            .filter { it.category == ConnectorRolloutStateCategory.ACTIVE }
            .sortedBy { it.literal }
            .toSet()

      val terminalStates: Set<ConnectorRolloutStateType>
        get() = ConnectorRolloutStateType.entries.filter { it.category == ConnectorRolloutStateCategory.TERMINAL }.toSet()
    }
  }

  internal enum class ConnectorRolloutStrategyType(
    private val literal: String,
  ) : EnumType {
    MANUAL("manual"),
    AUTOMATED("automated"),
    OVERRIDDEN("overridden"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String? = CONNECTOR_ROLLOUT_STRATEGY_TYPE

    override fun getLiteral(): String = literal
  }

  companion object {
    private val CONNECTOR_ROLLOUT_TABLE = DSL.table("connector_rollout")
    private val ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", UUID::class.java)
    private val STATE = DSL.field("state", String::class.java)
    private const val CONNECTOR_ROLLOUT_STATE_CATEGORY = "connector_rollout_state_category"
    private const val CONNECTOR_ROLLOUT_STATE_TYPE = "connector_rollout_state_type"
    private const val CONNECTOR_ROLLOUT_STRATEGY_TYPE = "connector_rollout_strategy_type"

    fun createStateTypeEnum(ctx: DSLContext) {
      ctx
        .createType(CONNECTOR_ROLLOUT_STATE_TYPE)
        .asEnum(
          ConnectorRolloutStateType.INITIALIZED.literal,
          ConnectorRolloutStateType.IN_PROGRESS.literal,
          ConnectorRolloutStateType.PAUSED.literal,
          ConnectorRolloutStateType.FINALIZING.literal,
          ConnectorRolloutStateType.SUCCEEDED.literal,
          ConnectorRolloutStateType.ERRORED.literal,
          ConnectorRolloutStateType.FAILED_ROLLED_BACK.literal,
          ConnectorRolloutStateType.CANCELED_ROLLED_BACK.literal,
        ).execute()
    }

    fun createStateCategoryEnum(ctx: DSLContext) {
      ctx
        .createType(CONNECTOR_ROLLOUT_STATE_CATEGORY)
        .asEnum(
          ConnectorRolloutStateCategory.ACTIVE.literal,
          ConnectorRolloutStateCategory.TERMINAL.literal,
        ).execute()
    }

    fun createStrategyTypeEnum(ctx: DSLContext) {
      ctx
        .createType(CONNECTOR_ROLLOUT_STRATEGY_TYPE)
        .asEnum(
          ConnectorRolloutStrategyType.MANUAL.literal,
          ConnectorRolloutStrategyType.AUTOMATED.literal,
          ConnectorRolloutStrategyType.OVERRIDDEN.literal,
        ).execute()
    }

    fun createRolloutTable(ctx: DSLContext) {
      ctx
        .createTableIfNotExists(CONNECTOR_ROLLOUT_TABLE)
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
          DSL
            .constraint("fk_actor_definition_id")
            .foreignKey("actor_definition_id")
            .references("actor_definition", "id"),
          DSL
            .constraint("fk_initial_version_id")
            .foreignKey("initial_version_id")
            .references("actor_definition_version", "id"),
          DSL
            .constraint("fk_release_candidate_version_id")
            .foreignKey("release_candidate_version_id")
            .references("actor_definition_version", "id"),
          DSL.constraint("fk_updated_by").foreignKey("updated_by").references("user", "id"),
        ).execute()
    }

    fun createPartialUniqueIndex(ctx: DSLContext) {
      // Create a partial unique index to guarantee that only one active rollout exists for a given
      // connector.
      ctx
        .createUniqueIndex("actor_definition_id_state_unique_idx")
        .on(CONNECTOR_ROLLOUT_TABLE, ACTOR_DEFINITION_ID)
        .where(STATE.`in`(ConnectorRolloutStateType.activeStates))
        .execute()
    }
  }
}
