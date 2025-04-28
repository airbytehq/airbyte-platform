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
 * Adds a missed unique constraint on (actor definition id, version) pair in the actor definition
 * version table. Removes duplicate rows if any.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_24_004__AddAndEnforceUniqueConstraintInADVTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    private val ACTOR_DEFINITION_VERSION: Table<*> = DSL.table("actor_definition_version")
    private val ID = DSL.field("id", UUID::class.java)
    private val ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", Int::class.java)
    private val DOCKER_IMAGE_TAG = DSL.field("docker_image_tag", String::class.java)
    private val CREATED_AT = DSL.field("created_at", Timestamp::class.java)

    @JvmStatic
    fun migrate(ctx: DSLContext) {
      dropDuplicateRows(ctx)
      dropNonUniqueIndex(ctx)
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
                      ACTOR_DEFINITION_ID,
                      DOCKER_IMAGE_TAG,
                    ).orderBy(CREATED_AT),
                ).`as`("creation_rank"),
            ).from(ACTOR_DEFINITION_VERSION)

        // Fetch IDs with creation_rank > 1 from the query
        val idsToDelete =
          transactionCtx
            .select(rankingQuery.field(ID))
            .from(rankingQuery)
            .where(rankingQuery.field("creation_rank", Int::class.java)!!.gt(1))
            .fetchInto(UUID::class.java)

        log.info { "Deleting ${idsToDelete.size} duplicate (on actor def id + docker image tag) rows from the ADV table." }

        // Delete rows based on fetched IDs
        if (idsToDelete.isNotEmpty()) {
          transactionCtx
            .deleteFrom(ACTOR_DEFINITION_VERSION)
            .where(ID.`in`(idsToDelete))
            .execute()
        }
      }
    }

    private fun dropNonUniqueIndex(ctx: DSLContext) {
      ctx
        .dropIndexIfExists("actor_definition_version_definition_image_tag_idx")
        .on(ACTOR_DEFINITION_VERSION)
        .execute()
    }

    private fun addUniqueConstraint(ctx: DSLContext) {
      ctx
        .alterTable(ACTOR_DEFINITION_VERSION)
        .add(
          DSL
            .constraint("actor_definition_version_actor_definition_id_version_key")
            .unique("actor_definition_id", "docker_image_tag"),
        ).execute()
    }
  }
}
