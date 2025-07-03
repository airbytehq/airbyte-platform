/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardSyncState
import io.airbyte.config.State
import io.airbyte.db.Database
import io.airbyte.db.factory.DSLContextFactory.create
import io.airbyte.db.factory.DatabaseDriver
import io.airbyte.db.legacy.ConfigSchema
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.Record2
import org.jooq.SQLDialect
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.time.OffsetDateTime
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Copy the latest job state for each standard sync to the config database.
 */
@Suppress("ktlint:standard:class-naming")
class V0_30_22_001__Store_last_sync_state : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    runCatching {
      val jobsDatabase =
        getJobsDatabase(
          context.configuration.user,
          context.configuration.password,
          context.configuration.url,
        )

      jobsDatabase?.let {
        copyData(ctx, getStandardSyncStates(it), OffsetDateTime.now())
      }
    }
  }

  /**
   * Get standard sync states.
   *
   * @return a set of StandardSyncStates from the latest attempt for each connection.
   */
  private fun getStandardSyncStates(jobsDatabase: Database): Set<StandardSyncState> {
    val jobsTable: Table<*> = DSL.table("jobs")
    val jobId = DSL.field("jobs.id", SQLDataType.BIGINT)
    val connectionId = DSL.field("jobs.scope", SQLDataType.VARCHAR)

    val attemptsTable: Table<*> = DSL.table("attempts")
    val attemptJobId = DSL.field("attempts.job_id", SQLDataType.BIGINT)
    val attemptCreatedAt = DSL.field("attempts.created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)

    // output schema: JobOutput.yaml
    // sync schema: StandardSyncOutput.yaml
    // state schema: State.yaml, e.g. { "state": { "cursor": 1000 } }
    val attemptState = DSL.field("attempts.output -> 'sync' -> 'state'", SQLDataType.JSONB)

    return jobsDatabase
      .query { ctx: DSLContext ->
        ctx
          .select(connectionId, attemptState)
          .distinctOn(connectionId)
          .from(attemptsTable)
          .innerJoin(jobsTable)
          .on(jobId.eq(attemptJobId))
          .where(attemptState.isNotNull()) // this query assumes that an attempt with larger created_at field is always a newer attempt
          .orderBy(connectionId, attemptCreatedAt.desc())
          .fetch()
          .stream()
          .map { r: Record2<String, JSONB> ->
            getStandardSyncState(
              UUID.fromString(r.value1()),
              Jsons.deserialize(
                r.value2().data(),
                State::class.java,
              ),
            )
          }
      }.collect(Collectors.toSet())
  }

  companion object {
    private const val MIGRATION_NAME = "Configs db migration 0.30.22.001"

    // airbyte configs table (we cannot use the jooq generated code here to avoid circular dependency)
    @JvmField
    val TABLE_AIRBYTE_CONFIGS: Table<*> = DSL.table("airbyte_configs")

    @JvmField
    val COLUMN_CONFIG_TYPE: Field<String> = DSL.field("config_type", SQLDataType.VARCHAR(60).nullable(false))

    @JvmField
    val COLUMN_CONFIG_ID: Field<String> = DSL.field("config_id", SQLDataType.VARCHAR(36).nullable(false))

    @JvmField
    val COLUMN_CONFIG_BLOB: Field<JSONB> = DSL.field("config_blob", SQLDataType.JSONB.nullable(false))

    @JvmField
    val COLUMN_CREATED_AT: Field<OffsetDateTime> = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE)

    @JvmField
    val COLUMN_UPDATED_AT: Field<OffsetDateTime> = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE)

    @JvmStatic
    fun copyData(
      ctx: DSLContext,
      standardSyncStates: Set<StandardSyncState>,
      timestamp: OffsetDateTime,
    ) {
      log.info { "[$MIGRATION_NAME] Number of connection states to copy: ${standardSyncStates.size}" }

      for (standardSyncState in standardSyncStates) {
        ctx
          .insertInto(TABLE_AIRBYTE_CONFIGS)
          .set(COLUMN_CONFIG_TYPE, ConfigSchema.STANDARD_SYNC_STATE.name)
          .set(COLUMN_CONFIG_ID, standardSyncState.connectionId.toString())
          .set(COLUMN_CONFIG_BLOB, JSONB.valueOf(Jsons.serialize(standardSyncState)))
          .set(COLUMN_CREATED_AT, timestamp)
          .set(
            COLUMN_UPDATED_AT,
            timestamp,
          ) // This migration is idempotent. If the record for a sync_id already exists,
          // it means that the migration has already been run before. Abort insertion.
          .onDuplicateKeyIgnore()
          .execute()
      }
    }

    /**
     * This migration requires a connection to the job database, which may be a separate database from
     * the config database. However, the job database only exists in production, not in development or
     * test. We use the job database environment variables to determine how to connect to the job
     * database. This approach is not 100% reliable. However, it is better than doing half of the
     * migration here (creating the table), and the rest of the work during server start up (copying the
     * data from the job database).
     */
    @JvmStatic
    @InternalForTesting
    fun getJobsDatabase(
      databaseUser: String,
      databasePassword: String,
      databaseUrl: String,
    ): Database? {
      try {
        require("" != databaseUrl.trim()) { "The databaseUrl cannot be empty." }
        // If the environment variables exist, it means the migration is run in production.
        // Connect to the official job database.
        val dslContext =
          create(
            databaseUser,
            databasePassword,
            DatabaseDriver.POSTGRESQL.driverClassName,
            databaseUrl,
            SQLDialect.POSTGRES,
          )
        val jobsDatabase = Database(dslContext)
        log.info { "[$MIGRATION_NAME] Connected to jobs database: $databaseUrl" }
        return jobsDatabase
      } catch (e: IllegalArgumentException) {
        // If the environment variables do not exist, it means the migration is run in development.
        log.info { "[$MIGRATION_NAME] This is the dev environment; there is no jobs database" }
        return null
      }
    }

    @JvmStatic
    @InternalForTesting
    fun getStandardSyncState(
      connectionId: UUID?,
      state: State?,
    ): StandardSyncState = StandardSyncState().withConnectionId(connectionId).withState(state)
  }
}
