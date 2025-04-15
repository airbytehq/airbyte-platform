/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.*;
import io.airbyte.config.*;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.secrets.*;
import io.airbyte.data.exceptions.*;
import io.airbyte.data.services.*;
import io.airbyte.featureflag.*;
import io.airbyte.metrics.*;
import io.airbyte.test.utils.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import org.jooq.*;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.*;
import org.junit.jupiter.api.*;
import org.mockito.*;

class OAuthServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final Table<Record> ACTOR_OAUTH_PARAMETER_TABLE = DSL.table("actor_oauth_parameter");
  private static final Field<UUID> ACTOR_DEFINITION_ID_COLUMN = DSL.field("actor_definition_id", SQLDataType.UUID);

  private static final JsonNode CONFIG = Jsons.deserialize("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}");
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID A_DIFFERENT_WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final UUID A_DIFFERENT_ORGANIZATION_ID = UUID.randomUUID();

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ID = UUID.randomUUID();
  private OAuthServiceJooqImpl oAuthService;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private SecretsRepositoryReader secretsRepositoryReader;

  private WorkspaceService workspaceService = mock(WorkspaceService.class);

  @BeforeEach
  void setUp() throws IOException, SQLException, ConfigNotFoundException {
    final FeatureFlagClient featureFlagClient = mock(TestClient.class, Mockito.withSettings().withoutAnnotations());
    secretsRepositoryReader = mock(SecretsRepositoryReader.class, Mockito.withSettings().withoutAnnotations());
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class, Mockito.withSettings().withoutAnnotations());
    final MetricClient metricClient = mock(MetricClient.class);

    workspaceService = mock(WorkspaceService.class);
    when(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID)).thenReturn(Optional.of(ORGANIZATION_ID));
    when(workspaceService.getOrganizationIdFromWorkspaceId(A_DIFFERENT_WORKSPACE_ID)).thenReturn(Optional.of(A_DIFFERENT_ORGANIZATION_ID));

    SecretPersistenceConfig secretPersistenceConfig = mock(SecretPersistenceConfig.class);
    when(secretPersistenceConfigService.get(ScopeType.ORGANIZATION, ORGANIZATION_ID)).thenReturn(secretPersistenceConfig);
    when(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(CONFIG)).thenReturn(CONFIG);

    oAuthService = new OAuthServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretPersistenceConfigService,
        metricClient,
        workspaceService);

    deleteActorOAuthParams();
  }

  private void deleteActorOAuthParams() throws SQLException {
    database.query(ctx -> ctx.deleteFrom(ACTOR_OAUTH_PARAMETER_TABLE)
        .where(ACTOR_DEFINITION_ID_COLUMN.eq(ACTOR_DEFINITION_ID))
        .execute());
  }

  @Nested
  class SourceOauthTests {

    @Test
    void testGetInstanceWideSourceOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertNull(fetchedOAuthParam.get().getWorkspaceId());
      assertNull(fetchedOAuthParam.get().getOrganizationId());
    }

    @Test
    void testGetNoInstanceWideSourceOAuthParam() throws IOException, ConfigNotFoundException {
      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);
      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testGetWorkspaceSourceOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertEquals(WORKSPACE_ID, fetchedOAuthParam.get().getWorkspaceId());
      assertNull(fetchedOAuthParam.get().getOrganizationId());
    }

    @Test
    void testGetWorkspaceSourceOAuthParamWithDifferentWorkspaceId() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam =
          oAuthService.getSourceOAuthParameterWithSecretsOptional(A_DIFFERENT_WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testGetOrganizationSourceOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertEquals(ORGANIZATION_ID, fetchedOAuthParam.get().getOrganizationId());
      assertNull(fetchedOAuthParam.get().getWorkspaceId());
    }

    @Test
    void testGetOrganizationSourceOAuthParamWrongOrg() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of(A_DIFFERENT_ORGANIZATION_ID), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testOrganizationSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE);
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(organizationWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testOrganizationSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.SOURCE);
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(organizationWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID workspaceWideParamId = UUID.randomUUID();

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE);
      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);

      SourceOAuthParameter orgWideParam = new SourceOAuthParameter()
          .withSourceDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(workspaceWideParamId)
          .withWorkspaceId(WORKSPACE_ID)
          .withConfiguration(CONFIG);
      oAuthService.writeSourceOAuthParam(orgWideParam);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID workspaceWideParamId = UUID.randomUUID();

      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedFirst()
        throws IOException, ConfigNotFoundException {
      final UUID workspaceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedLast()
        throws IOException, ConfigNotFoundException {
      final UUID workspaceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.SOURCE);
      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE);

      Optional<SourceOAuthParameter> fetchedOAuthParam = oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

  }

  @Nested
  class DestinationOAuthTests {

    @Test
    void testGetInstanceWideDestinatonOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertNull(fetchedOAuthParam.get().getWorkspaceId());
      assertNull(fetchedOAuthParam.get().getOrganizationId());
    }

    @Test
    void testGetNoInstanceWideDestinationOAuthParam() throws IOException, ConfigNotFoundException {
      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);
      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testGetWorkspaceDestinationOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertEquals(WORKSPACE_ID, fetchedOAuthParam.get().getWorkspaceId());
      assertNull(fetchedOAuthParam.get().getOrganizationId());
    }

    @Test
    void testGetWorkspaceDestinationOAuthParamWithDifferentWorkspaceId() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(A_DIFFERENT_WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testGetOrganizationDestinationOAuthParam() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());

      assertEquals(CONFIG, fetchedOAuthParam.get().getConfiguration());
      assertEquals(ORGANIZATION_ID, fetchedOAuthParam.get().getOrganizationId());
      assertNull(fetchedOAuthParam.get().getWorkspaceId());
    }

    @Test
    void testGetOrganizationDestinationOAuthParamWrongOrg() throws IOException, ConfigNotFoundException {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of(A_DIFFERENT_ORGANIZATION_ID), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertTrue(fetchedOAuthParam.isEmpty());
    }

    @Test
    void testOrganizationDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst()
        throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION);
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(organizationWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testOrganizationDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast()
        throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.DESTINATION);
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(organizationWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst()
        throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID workspaceWideParamId = UUID.randomUUID();

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION);
      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast()
        throws IOException, ConfigNotFoundException {
      final UUID instanceWideParamId = UUID.randomUUID();
      final UUID workspaceWideParamId = UUID.randomUUID();

      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedFirst()
        throws IOException, ConfigNotFoundException {
      final UUID workspaceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

    @Test
    void testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedLast()
        throws IOException, ConfigNotFoundException {
      final UUID workspaceWideParamId = UUID.randomUUID();
      final UUID organizationWideParamId = UUID.randomUUID();

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of(ORGANIZATION_ID), ActorType.DESTINATION);
      createActorOAuthParameter(workspaceWideParamId, Optional.of(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION);

      Optional<DestinationOAuthParameter> fetchedOAuthParam =
          oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID);

      assertFalse(fetchedOAuthParam.isEmpty());
      assertEquals(workspaceWideParamId, fetchedOAuthParam.get().getOauthParameterId());
    }

  }

  private void createActorOAuthParameter(UUID id, Optional<UUID> workspaceId, Optional<UUID> organizationId, ActorType actorType) throws IOException {
    if (actorType == ActorType.SOURCE) {
      SourceOAuthParameter param = new SourceOAuthParameter()
          .withSourceDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(id)
          .withWorkspaceId(workspaceId.orElse(null))
          .withOrganizationId(organizationId.orElse(null))
          .withConfiguration(CONFIG);
      oAuthService.writeSourceOAuthParam(param);
    } else {
      DestinationOAuthParameter param = new DestinationOAuthParameter()
          .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(id)
          .withWorkspaceId(workspaceId.orElse(null))
          .withOrganizationId(organizationId.orElse(null))
          .withConfiguration(CONFIG);
      oAuthService.writeDestinationOAuthParam(param);
    }
  }

}
