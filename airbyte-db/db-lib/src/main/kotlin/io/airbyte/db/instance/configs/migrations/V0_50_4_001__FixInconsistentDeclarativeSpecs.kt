/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record2
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * This migration fixes an inconsistency in specs for declarative sources. We were not updating the
 * spec in the actor_definition_version table when setting a new active declarative manifest
 * version, so the spec in the actor_definition_version table could be different from the spec in
 * the actor_definition table.
 *
 * This migration updates the spec in the actor_definition_version table to match the spec in the
 * actor_definition table to make sure things are consistent before we eventually fully remove the
 * field from the actor_definition table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_4_001__FixInconsistentDeclarativeSpecs : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateMismatchedSpecs(ctx)
  }

  private fun updateMismatchedSpecs(ctx: DSLContext) {
    // Look up all actor definitions whose spec is different from its default version's spec
    val mismatchedSpecs =
      ctx
        .select(ACTOR_DEFINITION_VERSION_ID, ACTOR_DEFINITION_SPEC)
        .from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_VERSION)
        .on(ACTOR_DEFINITION_VERSION_ID.eq(DEFAULT_VERSION_ID))
        .where(ACTOR_DEFINITION_VERSION_SPEC.ne(ACTOR_DEFINITION_SPEC))
        .fetch()

    // update actor_definition_version records with the spec from the actor_definition table
    mismatchedSpecs.forEach { record: Record2<UUID, JSONB> ->
      val actorDefVersionId = record.get(ACTOR_DEFINITION_VERSION_ID)
      val newSpec = record.get(ACTOR_DEFINITION_SPEC)
      ctx
        .update(ACTOR_DEFINITION_VERSION)
        .set(ACTOR_DEFINITION_VERSION_SPEC, newSpec)
        .where(ACTOR_DEFINITION_VERSION_ID.eq(actorDefVersionId))
        .execute()
    }
  }

  companion object {
    private val ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version")
    private val ACTOR_DEFINITION = DSL.table("actor_definition")
    private val DEFAULT_VERSION_ID = DSL.field(DSL.name("actor_definition", "default_version_id"), SQLDataType.UUID)
    private val ACTOR_DEFINITION_VERSION_ID = DSL.field(DSL.name("actor_definition_version", "id"), SQLDataType.UUID)
    private val ACTOR_DEFINITION_SPEC = DSL.field(DSL.name("actor_definition", "spec"), SQLDataType.JSONB)
    private val ACTOR_DEFINITION_VERSION_SPEC = DSL.field(DSL.name("actor_definition_version", "spec"), SQLDataType.JSONB)
  }
}
