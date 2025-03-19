/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import static io.airbyte.db.instance.configs.migrations.V1_1_1_017__AddOrganizationIdToActorOauthParameter.ONLY_WORKSPACE_OR_ORG_IS_SET;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.db.factory.FlywayFactory;
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest;
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.development.DevDatabaseMigrator;
import java.util.Map;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.IntegrityConstraintViolationException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Triple;

class V1_1_1_017__AddOrganizationIdToActorOauthParameterTest extends AbstractConfigsDatabaseTest {

  public static final JSONB CONFIGURATION = JSONB.valueOf("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}");
  static final Table<Record> ACTOR_OAUTH_PARAMETER = DSL.table("actor_oauth_parameter");
  static final Field<UUID> ID_FIELD = DSL.field("id", SQLDataType.UUID);
  static final Field<UUID> ACTOR_DEFINITION_ID_FIELD = DSL.field("actor_definition_id", SQLDataType.UUID);
  static final Field<UUID> ORGANIZATION_ID_FIELD = DSL.field("organization_id", SQLDataType.UUID);
  static final Field<UUID> WORKSPACE_ID_FIELD = DSL.field("workspace_id", SQLDataType.UUID);
  static final Field<JSONB> CONFIGURATION_FIELD = DSL.field("configuration", SQLDataType.JSONB);
  static final Field<ActorType> ACTOR_TYPE_FIELD = DSL.field("actor_type", SQLDataType.VARCHAR.asEnumDataType(ActorType.class));

  @BeforeEach
  void beforeEach() {
    final Flyway flyway =
        FlywayFactory.create(dataSource, "V1_1_1_017__AddOrganizationIdToActorOauthParameter", ConfigsDatabaseMigrator.DB_IDENTIFIER,
            ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION);

    final ConfigsDatabaseMigrator configsDbMigrator = new ConfigsDatabaseMigrator(database, flyway);
    final BaseJavaMigration previousMigration = new V1_1_1_016__AddDataplaneGroupIdToConnection();
    final DevDatabaseMigrator devConfigsDbMigrator = new DevDatabaseMigrator(configsDbMigrator, previousMigration.getVersion());
    devConfigsDbMigrator.createBaseline();

    final DSLContext ctx = getDslContext();
    dropConstraints(ctx);
    V1_1_1_017__AddOrganizationIdToActorOauthParameter.doMigration(ctx);
  }

  @Test
  void testCreateOverrideWithOrganizationId() {
    final DSLContext ctx = getDslContext();

    UUID id = UUID.randomUUID();
    UUID actorDefinitionId = UUID.randomUUID();
    UUID organizationId = UUID.randomUUID();
    JSONB configuration = JSONB.valueOf("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}");
    ActorType actorType = ActorType.source;

    ctx.insertInto(ACTOR_OAUTH_PARAMETER, ID_FIELD, ACTOR_DEFINITION_ID_FIELD, ORGANIZATION_ID_FIELD, CONFIGURATION_FIELD, ACTOR_TYPE_FIELD)
        .values(id, actorDefinitionId, organizationId, configuration, actorType)
        .execute();

    Map<UUID, Triple<UUID, UUID, JSONB>> oauthActorParamsMappings =
        ctx.select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
            .from(ACTOR_OAUTH_PARAMETER)
            .fetchMap(r -> r.get(ID_FIELD), r -> Triple.of(r.get(ORGANIZATION_ID_FIELD), r.get(WORKSPACE_ID_FIELD), r.get(CONFIGURATION_FIELD)));

    Assertions.assertEquals(organizationId, oauthActorParamsMappings.get(id).getLeft());
    Assertions.assertNull(oauthActorParamsMappings.get(id).getMiddle());
    Assertions.assertEquals(configuration, oauthActorParamsMappings.get(id).getRight());
  }

  @Test
  void testCreateOverrideWithWorkspaceId() {
    final DSLContext ctx = getDslContext();

    UUID id = UUID.randomUUID();
    UUID actorDefinitionId = UUID.randomUUID();
    UUID workspaceId = UUID.randomUUID();
    ActorType actorType = ActorType.source;

    ctx.insertInto(ACTOR_OAUTH_PARAMETER, ID_FIELD, ACTOR_DEFINITION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD, ACTOR_TYPE_FIELD)
        .values(id, actorDefinitionId, workspaceId, CONFIGURATION, actorType)
        .execute();

    Map<UUID, Triple<UUID, UUID, JSONB>> oauthActorParamsMappings =
        ctx.select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
            .from(ACTOR_OAUTH_PARAMETER)
            .fetchMap(r -> r.get(ID_FIELD), r -> Triple.of(r.get(ORGANIZATION_ID_FIELD), r.get(WORKSPACE_ID_FIELD), r.get(CONFIGURATION_FIELD)));

    Assertions.assertNull(oauthActorParamsMappings.get(id).getLeft());
    Assertions.assertEquals(workspaceId, oauthActorParamsMappings.get(id).getMiddle());
    Assertions.assertEquals(CONFIGURATION, oauthActorParamsMappings.get(id).getRight());
  }

  @Test
  void testCreateOverrideWithOrganizationIdAndWorkspaceId() {
    final DSLContext ctx = getDslContext();

    UUID id = UUID.randomUUID();
    UUID actorDefinitionId = UUID.randomUUID();
    UUID workspaceId = UUID.randomUUID();
    UUID organizationId = UUID.randomUUID();
    ActorType actorType = ActorType.source;

    assertThrows(IntegrityConstraintViolationException.class, () -> {
      ctx.insertInto(ACTOR_OAUTH_PARAMETER, ID_FIELD, ACTOR_DEFINITION_ID_FIELD, WORKSPACE_ID_FIELD, ORGANIZATION_ID_FIELD, CONFIGURATION_FIELD,
          ACTOR_TYPE_FIELD)
          .values(id, actorDefinitionId, workspaceId, organizationId, CONFIGURATION, actorType)
          .execute();
    });
  }

  @Test
  void testCreateOverrideWithoutOrganizationIdOrWorkspaceId() {
    final DSLContext ctx = getDslContext();

    UUID id = UUID.randomUUID();
    UUID actorDefinitionId = UUID.randomUUID();
    ActorType actorType = ActorType.source;

    ctx.insertInto(ACTOR_OAUTH_PARAMETER, ID_FIELD, ACTOR_DEFINITION_ID_FIELD, CONFIGURATION_FIELD, ACTOR_TYPE_FIELD)
        .values(id, actorDefinitionId, CONFIGURATION, actorType)
        .execute();

    Map<UUID, Triple<UUID, UUID, JSONB>> oauthActorParamsMappings =
        ctx.select(ID_FIELD, ORGANIZATION_ID_FIELD, WORKSPACE_ID_FIELD, CONFIGURATION_FIELD)
            .from(ACTOR_OAUTH_PARAMETER)
            .fetchMap(r -> r.get(ID_FIELD), r -> Triple.of(r.get(ORGANIZATION_ID_FIELD), r.get(WORKSPACE_ID_FIELD), r.get(CONFIGURATION_FIELD)));

    Assertions.assertNull(oauthActorParamsMappings.get(id).getLeft());
    Assertions.assertNull(oauthActorParamsMappings.get(id).getMiddle());
    Assertions.assertEquals(CONFIGURATION, oauthActorParamsMappings.get(id).getRight());
  }

  private static void dropConstraints(final DSLContext ctx) {
    ctx.alterTable(ACTOR_OAUTH_PARAMETER)
        .dropConstraintIfExists(ONLY_WORKSPACE_OR_ORG_IS_SET)
        .execute();
  }

}
