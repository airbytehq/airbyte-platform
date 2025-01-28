/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import io.airbyte.db.instance.configs.migrations.V0_35_15_001__AddReleaseStageAndReleaseDateToActorDefinition.ReleaseStage;
import java.sql.Date;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.EnumConverter;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Backfill actor definition versions with existing data from actor definitions.
 */
public class V0_44_5_003__BackfillActorDefinitionVersion extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_44_5_003__BackfillActorDefinitionVersion.class);

  private static final Table<Record> ACTOR_DEFINITION_VERSION = DSL.table("actor_definition_version");
  private static final Table<Record> ACTOR_DEFINITION = DSL.table("actor_definition");

  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> ACTOR_DEFINITION_ID = DSL.field("actor_definition_id", SQLDataType.UUID);
  private static final Field<String> DOCUMENTATION_URL = DSL.field("documentation_url", SQLDataType.VARCHAR);
  private static final Field<String> DOCKER_REPOSITORY = DSL.field("docker_repository", SQLDataType.VARCHAR);
  private static final Field<String> DOCKER_IMAGE_TAG = DSL.field("docker_image_tag", SQLDataType.VARCHAR);
  private static final Field<JSONB> SPEC = DSL.field("spec", SQLDataType.JSONB);
  private static final Field<String> PROTOCOL_VERSION = DSL.field("protocol_version", SQLDataType.VARCHAR);
  private static final Field<String> NORMALIZATION_REPOSITORY = DSL.field("normalization_repository", SQLDataType.VARCHAR);
  private static final Field<String> NORMALIZATION_TAG = DSL.field("normalization_tag", SQLDataType.VARCHAR);
  private static final Field<Boolean> SUPPORTS_DBT = DSL.field("supports_dbt", SQLDataType.BOOLEAN);
  private static final Field<String> NORMALIZATION_INTEGRATION_TYPE = DSL.field("normalization_integration_type", SQLDataType.VARCHAR);
  private static final Field<JSONB> ALLOWED_HOSTS = DSL.field("allowed_hosts", SQLDataType.JSONB);
  private static final Field<JSONB> SUGGESTED_STREAMS = DSL.field("suggested_streams", SQLDataType.JSONB);
  private static final Field<ReleaseStage> RELEASE_STAGE = DSL.field("release_stage", SQLDataType.VARCHAR.asEnumDataType(ReleaseStage.class));
  private static final Field<Date> RELEASE_DATE = DSL.field("release_date", SQLDataType.DATE);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    backfillActorDefinitionVersion(ctx);
  }

  private void backfillActorDefinitionVersion(final DSLContext ctx) {
    final var actorDefinitions = ctx.select()
        .from(ACTOR_DEFINITION)
        .where(DSL.field("default_version_id").isNull())
        .fetch();

    for (final var actorDefinition : actorDefinitions) {
      final UUID newVersionId = UUID.randomUUID();
      ctx.insertInto(ACTOR_DEFINITION_VERSION)
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
          .set(RELEASE_STAGE, actorDefinition.get("release_stage", new EnumConverter<>(String.class, ReleaseStage.class)))
          .set(RELEASE_DATE, actorDefinition.get(RELEASE_DATE))
          .execute();

      ctx.update(ACTOR_DEFINITION)
          .set(DSL.field("default_version_id"), newVersionId)
          .where(ID.eq(actorDefinition.get(ID)))
          .execute();
    }
  }

}
