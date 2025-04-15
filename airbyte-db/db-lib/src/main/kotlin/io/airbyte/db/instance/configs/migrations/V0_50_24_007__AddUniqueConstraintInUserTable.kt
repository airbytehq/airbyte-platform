/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Configuration
import org.jooq.DSLContext
import org.jooq.Select
import org.jooq.Table
import org.jooq.impl.DSL
import java.sql.Timestamp
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Adds a missed unique constraint on (auth_user_id, auth_provider) pair in User table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_007__AddUniqueConstraintInUserTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    migrate(ctx)
    log.info { "Migration finished!" }
  }

  companion object {
    val USER_TABLE: Table<*> = DSL.table(""""user"""") // Using quotes in case it conflicts with the reserved "user" keyword in Postgres.
    private val ID = DSL.field("id", UUID::class.java)
    private val AUTH_USER_ID_FIELD = DSL.field("auth_user_id", String::class.java)
    private val AUTH_PROVIDER_FIELD = DSL.field("auth_provider", V0_50_16_001__UpdateEnumTypeAuthProviderAndPermissionType.AuthProvider::class.java)
    private val CREATED_AT = DSL.field("created_at", Timestamp::class.java)

    @JvmStatic
    fun migrate(ctx: DSLContext) {
      dropDuplicateRows(ctx)
      addUniqueConstraint(ctx)
    }

    private fun dropDuplicateRows(ctx: DSLContext) {
      ctx.transaction { configuration: Configuration? ->
        val transactionCtx = DSL.using(configuration)
        // Define the ranking logic within a select query
        val rankingQuery: Select<*> =
          transactionCtx
            .select(
              ID,
              DSL
                .rowNumber()
                .over(
                  DSL
                    .partitionBy(
                      AUTH_USER_ID_FIELD,
                      AUTH_PROVIDER_FIELD,
                    ).orderBy(CREATED_AT),
                ).`as`("creation_rank"),
            ).from(USER_TABLE)

        // Fetch IDs with creation_rank > 1 from the query
        val userIdsToDelete =
          transactionCtx
            .select(
              rankingQuery.field(ID),
            ).from(rankingQuery)
            .where(rankingQuery.field("creation_rank", Int::class.java)!!.gt(1))
            .fetchInto(UUID::class.java)

        log.info { "Deleting ${userIdsToDelete.size} duplicate (auth_user_id, auth_provider) rows from the User table." }

        // Delete rows based on fetched IDs
        if (userIdsToDelete.isNotEmpty()) {
          transactionCtx
            .deleteFrom(USER_TABLE)
            .where(ID.`in`(userIdsToDelete))
            .execute()
        }
      }
    }

    private fun addUniqueConstraint(ctx: DSLContext) {
      ctx
        .alterTable(USER_TABLE)
        .add(DSL.constraint("auth_user_id_auth_provider_key").unique(AUTH_USER_ID_FIELD, AUTH_PROVIDER_FIELD))
        .execute()
    }
  }
}
