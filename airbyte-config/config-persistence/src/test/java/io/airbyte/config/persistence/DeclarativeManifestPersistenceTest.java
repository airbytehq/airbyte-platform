/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ActorDefinitionService;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeclarativeManifestPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID AN_ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ANOTHER_ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final Long A_VERSION = 1L;
  private static final Long ANOTHER_VERSION = 2L;
  private static final String A_CDK_VERSION = "0.29.0";
  private static final JsonNode A_MANIFEST;
  private static final JsonNode ANOTHER_MANIFEST;
  private static final JsonNode A_SPEC;
  private static final JsonNode ANOTHER_SPEC;

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      ANOTHER_MANIFEST =
          new ObjectMapper().readTree("{\"another_manifest\": \"another_manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
      ANOTHER_SPEC = new ObjectMapper().readTree("{\"another_spec\": \"another_spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConnectorBuilderService connectorBuilderService;
  private SourceService sourceService;
  private ActorDefinitionService actorDefinitionService;
  private WorkspaceService workspaceService;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    final ConnectionService connectionService = mock(ConnectionService.class);
    final ScopedConfigurationService scopedConfigurationService = mock(ScopedConfigurationService.class);
    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    final ConnectionTimelineEventService connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService, scopedConfigurationService,
            connectionTimelineEventService);

    sourceService = new SourceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater);
    workspaceService = new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter,
        secretPersistenceConfigService);
    connectorBuilderService = new ConnectorBuilderServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());
  }

  @Test
  void whenInsertDeclarativeManifestThenEntryIsInDb() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest manifest = MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    connectorBuilderService.insertDeclarativeManifest(manifest);
    assertEquals(manifest, connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION));
  }

  @Test
  void givenActorDefinitionIdAndVersionAlreadyInDbWhenInsertDeclarativeManifestThenThrowException() throws IOException {
    final DeclarativeManifest manifest = MockData.declarativeManifest();
    connectorBuilderService.insertDeclarativeManifest(manifest);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.insertDeclarativeManifest(manifest));
  }

  @Test
  void givenManifestIsNullWhenInsertDeclarativeManifestThenThrowException() {
    final DeclarativeManifest declarativeManifestWithoutManifest = MockData.declarativeManifest().withManifest(null);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.insertDeclarativeManifest(declarativeManifestWithoutManifest));
  }

  @Test
  void givenSpecIsNullWhenInsertDeclarativeManifestThenThrowException() {
    final DeclarativeManifest declarativeManifestWithoutManifest = MockData.declarativeManifest().withSpec(null);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.insertDeclarativeManifest(declarativeManifestWithoutManifest));
  }

  @Test
  void whenGetDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifestWithoutManifestAndSpec() throws IOException {
    final DeclarativeManifest declarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(A_SPEC);
    connectorBuilderService.insertDeclarativeManifest(declarativeManifest);

    final DeclarativeManifest result =
        connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).findFirst().orElse(null);

    assertEquals(declarativeManifest.withManifest(null).withSpec(null), result);
  }

  @Test
  void givenManyEntriesMatchingWhenGetDeclarativeManifestsByActorDefinitionIdThenReturnAllEntries() throws IOException {
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(1L));
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(2L));

    final List<DeclarativeManifest> manifests = connectorBuilderService.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).toList();

    assertEquals(2, manifests.size());
  }

  @Test
  void whenGetDeclarativeManifestByActorDefinitionIdAndVersionThenReturnDeclarativeManifest() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest declarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    connectorBuilderService.insertDeclarativeManifest(declarativeManifest);

    final DeclarativeManifest result = connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION);

    assertEquals(declarativeManifest, result);
  }

  @Test
  void givenNoDeclarativeManifestMatchingWhenGetDeclarativeManifestByActorDefinitionIdAndVersionThenThrowException() {
    assertThrows(ConfigNotFoundException.class,
        () -> connectorBuilderService.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION));
  }

  @Test
  void whenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest activeDeclarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    connectorBuilderService
        .insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION));
    connectorBuilderService.insertActiveDeclarativeManifest(activeDeclarativeManifest);

    final DeclarativeManifest result = connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID);

    assertEquals(activeDeclarativeManifest, result);
  }

  @Test
  void givenNoActiveManifestWhenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException {
    connectorBuilderService
        .insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));
    assertThrows(ConfigNotFoundException.class,
        () -> connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

  @Test
  void whenCreateDeclarativeManifestAsActiveVersionThenUpdateSourceDefinitionAndConfigInjectionAndDeclarativeManifest()
      throws IOException, ConfigNotFoundException, JsonValidationException {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    final DeclarativeManifest declarativeManifest = MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(createSpec(A_SPEC));
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withJsonToInject(A_MANIFEST);
    final ConnectorSpecification connectorSpecification = MockData.connectorSpecification().withConnectionSpecification(A_SPEC);

    connectorBuilderService.createDeclarativeManifestAsActiveVersion(declarativeManifest, configInjection, connectorSpecification, A_CDK_VERSION);

    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(A_CDK_VERSION, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getDockerImageTag());
    assertEquals(List.of(configInjection), connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(declarativeManifest, connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(DataAccessException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenActorDefinitionIdMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenManifestMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(ANOTHER_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenSpecMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(ANOTHER_SPEC), A_CDK_VERSION));
  }

  @Test
  void whenSetDeclarativeSourceActiveVersionThenUpdateSourceDefinitionAndConfigInjectionAndActiveDeclarativeManifest() throws Exception {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION));
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withJsonToInject(A_MANIFEST);
    final ConnectorSpecification connectorSpecification = MockData.connectorSpecification().withConnectionSpecification(A_SPEC);

    connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID, A_VERSION, configInjection, connectorSpecification,
        A_CDK_VERSION);

    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(A_CDK_VERSION, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getDockerImageTag());
    assertEquals(List.of(configInjection), connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(A_VERSION, connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).getVersion());
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() {
    assertThrows(DataAccessException.class, () -> connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        MockData.actorDefinitionConfigInjection(),
        MockData.connectorSpecification(), A_CDK_VERSION));
  }

  @Test
  void givenActiveDeclarativeManifestDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() throws Exception {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID),
        MockData.connectorSpecification(), A_CDK_VERSION));
  }

  void givenActiveDeclarativeManifestWithActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    final Long version = 4L;
    connectorBuilderService
        .insertActiveDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(actorDefinitionId).withVersion(version));
  }

  void givenSourceDefinition(final UUID sourceDefinitionId) throws JsonValidationException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    workspaceService.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(workspaceId));
    sourceService.writeCustomConnectorMetadata(
        MockData.customSourceDefinition().withSourceDefinitionId(sourceDefinitionId),
        MockData.actorDefinitionVersion().withActorDefinitionId(sourceDefinitionId),
        workspaceId,
        ScopeType.WORKSPACE);
  }

  JsonNode createSpec(final JsonNode connectionSpecification) {
    return new ObjectMapper().createObjectNode().set("connectionSpecification", connectionSpecification);
  }

}
