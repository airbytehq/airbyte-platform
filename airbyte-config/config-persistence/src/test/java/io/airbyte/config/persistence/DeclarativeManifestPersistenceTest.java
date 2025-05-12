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
import io.airbyte.config.DataplaneGroup;
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
import io.airbyte.data.services.DataplaneGroupService;
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
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
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
  private static final String A_MANIFEST_KEY = "__injected_declarative_manifest";
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
    final MetricClient metricClient = mock(MetricClient.class);
    final DataplaneGroupService dataplaneGroupService = mock(DataplaneGroupService.class);
    when(dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(any(), any()))
        .thenReturn(new DataplaneGroup().withId(UUID.randomUUID()));

    sourceService = new SourceServiceJooqImpl(database, featureFlagClient,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater, metricClient);
    workspaceService = new WorkspaceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter,
        secretPersistenceConfigService, metricClient);
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
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST);
    final ConnectorSpecification connectorSpecification = MockData.connectorSpecification().withConnectionSpecification(A_SPEC);

    connectorBuilderService.createDeclarativeManifestAsActiveVersion(declarativeManifest, List.of(configInjection), connectorSpecification,
        A_CDK_VERSION);

    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(A_CDK_VERSION, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getDockerImageTag());
    assertEquals(List.of(configInjection), connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(declarativeManifest, connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        List.of(configInjection),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenActorDefinitionIdMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        List.of(MockData.actorDefinitionConfigInjection().withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST)),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenManifestMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        List.of(MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(ANOTHER_MANIFEST)),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC), A_CDK_VERSION));
  }

  @Test
  void givenSpecMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> connectorBuilderService.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        List.of(MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST)),
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
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST);
    final ConnectorSpecification connectorSpecification = MockData.connectorSpecification().withConnectionSpecification(A_SPEC);

    connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID, A_VERSION, List.of(configInjection), connectorSpecification,
        A_CDK_VERSION);

    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(A_CDK_VERSION, actorDefinitionService.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getDockerImageTag());
    assertEquals(List.of(configInjection), connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(A_VERSION, connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).getVersion());
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() {
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST);

    assertThrows(DataAccessException.class, () -> connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        List.of(configInjection),
        MockData.connectorSpecification(), A_CDK_VERSION));
  }

  @Test
  void givenActiveDeclarativeManifestDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() throws Exception {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST);
    assertThrows(DataAccessException.class, () -> connectorBuilderService.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        List.of(configInjection),
        MockData.connectorSpecification(), A_CDK_VERSION));
  }

  @Test
  void whenSetDeclarativeSourceActiveVersionMultipleTimesThenConfigInjectionsAreReplaced() throws Exception {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);

    // Insert initial manifest
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION));

    // Create initial set of 3 config injections
    final List<ActorDefinitionConfigInjection> initialConfigInjections = List.of(
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath(A_MANIFEST_KEY)
            .withJsonToInject(A_MANIFEST),
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath("path2")
            .withJsonToInject(A_MANIFEST),
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath("path3")
            .withJsonToInject(A_MANIFEST));

    // First call to setDeclarativeSourceActiveVersion with 3 injections
    connectorBuilderService.setDeclarativeSourceActiveVersion(
        AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        initialConfigInjections,
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC),
        A_CDK_VERSION);

    // Verify all 3 initial injections were added
    assertEquals(3, connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).count());

    // Create new single config injection
    final List<ActorDefinitionConfigInjection> replacementConfigInjection = List.of(
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath(A_MANIFEST_KEY)
            .withJsonToInject(A_MANIFEST));

    // Second call to setDeclarativeSourceActiveVersion with 1 injection
    connectorBuilderService.setDeclarativeSourceActiveVersion(
        AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        replacementConfigInjection,
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC),
        A_CDK_VERSION);

    // Verify only 1 injection remains
    final List<ActorDefinitionConfigInjection> remainingInjections =
        connectorBuilderService.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList();
    assertEquals(1, remainingInjections.size());
    assertEquals(A_MANIFEST_KEY, remainingInjections.get(0).getInjectionPath());
  }

  @Test
  void whenSetDeclarativeSourceActiveVersionWithMixedActorDefinitionIdsThenThrowException() throws Exception {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);

    // Insert initial manifest
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION));

    // Create config injections with different actor definition IDs
    final List<ActorDefinitionConfigInjection> mixedConfigInjections = List.of(
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath(A_MANIFEST_KEY)
            .withJsonToInject(A_MANIFEST),
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID) // Different actor definition ID
            .withInjectionPath("path2")
            .withJsonToInject(A_MANIFEST));

    // Verify that calling setDeclarativeSourceActiveVersion with mixed actor definition IDs throws
    // exception
    assertThrows(IllegalArgumentException.class,
        () -> connectorBuilderService.setDeclarativeSourceActiveVersion(
            AN_ACTOR_DEFINITION_ID,
            A_VERSION,
            mixedConfigInjections,
            MockData.connectorSpecification().withConnectionSpecification(A_SPEC),
            A_CDK_VERSION));
  }

  @Test
  void whenSetDeclarativeSourceActiveVersionWithoutManifestInjectionThenThrowException() throws Exception {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);

    // Insert initial manifest
    connectorBuilderService.insertDeclarativeManifest(MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION));

    // Create config injections without manifest injection
    final List<ActorDefinitionConfigInjection> configInjectionsWithoutManifest = List.of(
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath("path1")
            .withJsonToInject(A_MANIFEST),
        MockData.actorDefinitionConfigInjection()
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withInjectionPath("path2")
            .withJsonToInject(A_MANIFEST));

    // Verify that calling setDeclarativeSourceActiveVersion without manifest injection throws exception
    assertThrows(IllegalArgumentException.class,
        () -> connectorBuilderService.setDeclarativeSourceActiveVersion(
            AN_ACTOR_DEFINITION_ID,
            A_VERSION,
            configInjectionsWithoutManifest,
            MockData.connectorSpecification().withConnectionSpecification(A_SPEC),
            A_CDK_VERSION));
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
