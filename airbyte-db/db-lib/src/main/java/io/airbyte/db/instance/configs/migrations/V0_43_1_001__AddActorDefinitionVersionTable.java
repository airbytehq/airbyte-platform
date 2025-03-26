/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static org.jooq.impl.DSL.currentOffsetDateTime;
import static org.jooq.impl.DSL.foreignKey;
import static org.jooq.impl.DSL.primaryKey;

import java.sql.Date;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add actor_definition_version table.
 */
public class V0_43_1_001__AddActorDefinitionVersionTable extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_43_1_001__AddActorDefinitionVersionTable.class);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());
    final DSLContext ctx = DSL.using(context.getConnection());
    createActorDefinitionVersionTable(ctx);
    ctx.createIndexIfNotExists("actor_definition_version_definition_image_tag_idx")
        .on("actor_definition_version", "actor_definition_id", "docker_image_tag")
        .execute();
  }

  private static void createActorDefinitionVersionTable(final DSLContext ctx) {
    final Field<UUID> id = DSL.field("id", SQLDataType.UUID.nullable(false));
    final Field<UUID> actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false));
    final Field<OffsetDateTime> createdAt =
        DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<OffsetDateTime> updatedAt =
        DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(currentOffsetDateTime()));
    final Field<String> documentationUrl = DSL.field("documentation_url", SQLDataType.VARCHAR(256).nullable(true));
    final Field<String> dockerRepository = DSL.field("docker_repository", SQLDataType.VARCHAR(256).nullable(false));
    final Field<String> dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR(256).nullable(false));
    final Field<JSONB> spec = DSL.field("spec", SQLDataType.JSONB.nullable(false));
    final Field<String> protocolVersion = DSL.field("protocol_version", SQLDataType.VARCHAR(256).nullable(true));
    final Field<String> releaseStage = DSL.field("release_stage", SQLDataType.VARCHAR(256).nullable(true));
    final Field<Date> releaseDate = DSL.field("release_date", SQLDataType.DATE.nullable(true));
    final Field<String> normalizationRepository = DSL.field("normalization_repository", SQLDataType.VARCHAR(256).nullable(true));
    final Field<String> normalizationTag = DSL.field("normalization_tag", SQLDataType.VARCHAR(256).nullable(true));
    final Field<Boolean> supportsDbt = DSL.field("supports_dbt", SQLDataType.BOOLEAN.nullable(true));
    final Field<String> normalizationIntegrationType = DSL.field("normalization_integration_type", SQLDataType.VARCHAR(256).nullable(true));
    final Field<JSONB> allowedHosts = DSL.field("allowed_hosts", SQLDataType.JSONB.nullable(true));

    ctx.createTableIfNotExists("actor_definition_version")
        .columns(id,
            actorDefinitionId,
            createdAt,
            updatedAt,
            documentationUrl,
            dockerRepository,
            dockerImageTag,
            spec,
            protocolVersion,
            releaseStage,
            releaseDate,
            normalizationRepository,
            normalizationTag,
            supportsDbt,
            normalizationIntegrationType,
            allowedHosts)
        .constraints(primaryKey(id),
            foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade())
        .execute();
  }

}
