/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.EnumConverter
import org.jooq.impl.SQLDataType
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Backfill actor definition versions with existing data from actor definitions.
 */
@Suppress("ktlint:standard:class-naming")
class V0_44_5_003__BackfillActorDefinitionVersion : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    backfillActorDefinitionVersion(ctx)
  }

  private fun backfillActorDefinitionVersion(ctx: DSLContext) {
    val actorDefinitions =
      ctx
        .select()
        .from(ACTOR_DEFINITION)
        .where(DSL.field("default_version_id").isNull())
        .fetch()

    for (actorDefinition in actorDefinitions) {
      val newVersionId = UUID.randomUUID()
      ctx
        .insertInto(ACTOR_DEFINITION_VERSION)
        .set(ID, newVersionId)
        .set(ACTOR_DEFINITION_ID, actorDefinition.get(ID))
        .set(DOCUMENTATION_URL, actorDefinition.get(DOCUMENTATION_URL))
        .set(DOCKER_REPOSITORY, actorDefinition.get(DOCKER_REPOSITORY))
        .set(DOCKER_IMAGE_TAG, actorDefinition.get(DOCKER_IMAGE_TAG))
        .set(SPEC, actorDefinition.get(SPEC))
        .set(PROTOCOL_VERSION, actorDefinition.get(PROTOCOL_VERSION))
        .set(NORMALIZATION_REPOSITORY, actorDefinition.get(NORMALIZATION_REPOSITORY))
        .set(NORMALIZATION_TAG, actorDefinition.get(NORMALIZATION_TAG))
        .set(SUPPORTS_DBT, actorDefinition.get(SUPPORTS_DBT))
        .set(NORMALIZATION_INTEGRATION_TYPE, actorDefinition.get(NORMALIZATION_INTEGRATION_TYPE))
        .set(ALLOWED_HOSTS, actorDefinition.get(ALLOWED_HOSTS))
        .set(SUGGESTED_STREAMS, actorDefinition.get(SUGGESTED_STREAMS))
        .set(
          RELEASE_STAGE,
          actorDefinition.get(
            "release_stage",
            EnumConverter(
              String::class.java,
              V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage::class.java,
            ),
          ),
        ).set(RELEASE_DATE, actorDefinition.get(RELEASE_DATE))
        .execute()

      ctx
        .update(ACTOR_DEFINITION)
        .set(DSL.field("default_version_id"), newVersionId)
        .where(ID.eq(actorDefinition.get(ID)))
        .execute()
    }
  }

  companion object {
    private val ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version")
    private val ACTOR_DEFINITION = DSL.table("actor_definition")

    private val ID = DSL.field("id", SQLDataType.UUID)
    private val ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", SQLDataType.UUID)
    private val DOCUMENTATION_URL = DSL.field("documentation_url", SQLDataType.VARCHAR)
    private val DOCKER_REPOSITORY = DSL.field("docker_repository", SQLDataType.VARCHAR)
    private val DOCKER_IMAGE_TAG = DSL.field("docker_image_tag", SQLDataType.VARCHAR)
    private val SPEC = DSL.field("spec", SQLDataType.JSONB)
    private val PROTOCOL_VERSION = DSL.field("protocol_version", SQLDataType.VARCHAR)
    private val NORMALIZATION_REPOSITORY = DSL.field("normalization_repository", SQLDataType.VARCHAR)
    private val NORMALIZATION_TAG = DSL.field("normalization_tag", SQLDataType.VARCHAR)
    private val SUPPORTS_DBT = DSL.field("supports_dbt", SQLDataType.BOOLEAN)
    private val NORMALIZATION_INTEGRATION_TYPE = DSL.field("normalization_integration_type", SQLDataType.VARCHAR)
    private val ALLOWED_HOSTS = DSL.field("allowed_hosts", SQLDataType.JSONB)
    private val SUGGESTED_STREAMS = DSL.field("suggested_streams", SQLDataType.JSONB)
    private val RELEASE_STAGE =
      DSL.field(
        "release_stage",
        SQLDataType.VARCHAR.asEnumDataType(
          V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage::class.java,
        ),
      )
    private val RELEASE_DATE = DSL.field("release_date", SQLDataType.DATE)
  }
}
