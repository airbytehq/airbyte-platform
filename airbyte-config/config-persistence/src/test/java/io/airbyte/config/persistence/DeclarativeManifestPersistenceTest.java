/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.config.ScopeType;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.CatalogServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.HealthCheckServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OAuthServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OperationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
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
  private static final JsonNode A_MANIFEST;
  private static final JsonNode ANOTHER_MANIFEST;
  private static final JsonNode A_SPEC;
  private static final JsonNode ANOTHER_SPEC;

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      ANOTHER_MANIFEST = new ObjectMapper().readTree("{\"another_manifest\": \"another_manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
      ANOTHER_SPEC = new ObjectMapper().readTree("{\"another_spec\": \"another_spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);

    configRepository = new ConfigRepository(
        new ActorDefinitionServiceJooqImpl(database),
        new CatalogServiceJooqImpl(database),
        new ConnectionServiceJooqImpl(database),
        new ConnectorBuilderServiceJooqImpl(database),
        new DestinationServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new HealthCheckServiceJooqImpl(database),
        new OAuthServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretPersistenceConfigService),
        new OperationServiceJooqImpl(database),
        new OrganizationServiceJooqImpl(database),
        new SourceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService),
        new WorkspaceServiceJooqImpl(database,
            featureFlagClient,
            secretsRepositoryReader,
            secretsRepositoryWriter,
            secretPersistenceConfigService));
  }

  @Test
  void whenInsertDeclarativeManifestThenEntryIsInDb() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest manifest = MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    configRepository.insertDeclarativeManifest(manifest);
    assertEquals(manifest, configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION));
  }

  @Test
  void givenActorDefinitionIdAndVersionAlreadyInDbWhenInsertDeclarativeManifestThenThrowException() throws IOException {
    final DeclarativeManifest manifest = MockData.declarativeManifest();
    configRepository.insertDeclarativeManifest(manifest);
    assertThrows(DataAccessException.class, () -> configRepository.insertDeclarativeManifest(manifest));
  }

  @Test
  void givenManifestIsNullWhenInsertDeclarativeManifestThenThrowException() {
    final DeclarativeManifest declarativeManifestWithoutManifest = MockData.declarativeManifest().withManifest(null);
    assertThrows(DataAccessException.class, () -> configRepository.insertDeclarativeManifest(declarativeManifestWithoutManifest));
  }

  @Test
  void givenSpecIsNullWhenInsertDeclarativeManifestThenThrowException() {
    final DeclarativeManifest declarativeManifestWithoutManifest = MockData.declarativeManifest().withSpec(null);
    assertThrows(DataAccessException.class, () -> configRepository.insertDeclarativeManifest(declarativeManifestWithoutManifest));
  }

  @Test
  void whenGetDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifestWithoutManifestAndSpec() throws IOException {
    final DeclarativeManifest declarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(A_SPEC);
    configRepository.insertDeclarativeManifest(declarativeManifest);

    final DeclarativeManifest result = configRepository.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).findFirst().orElse(null);

    assertEquals(declarativeManifest.withManifest(null).withSpec(null), result);
  }

  @Test
  void givenManyEntriesMatchingWhenGetDeclarativeManifestsByActorDefinitionIdThenReturnAllEntries() throws IOException {
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(1L));
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(2L));

    final List<DeclarativeManifest> manifests = configRepository.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).toList();

    assertEquals(2, manifests.size());
  }

  @Test
  void whenGetDeclarativeManifestByActorDefinitionIdAndVersionThenReturnDeclarativeManifest() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest declarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    configRepository.insertDeclarativeManifest(declarativeManifest);

    final DeclarativeManifest result = configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION);

    assertEquals(declarativeManifest, result);
  }

  @Test
  void givenNoDeclarativeManifestMatchingWhenGetDeclarativeManifestByActorDefinitionIdAndVersionThenThrowException() {
    assertThrows(ConfigNotFoundException.class,
        () -> configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION));
  }

  @Test
  void whenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest activeDeclarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    configRepository
        .insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION));
    configRepository.insertActiveDeclarativeManifest(activeDeclarativeManifest);

    final DeclarativeManifest result = configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID);

    assertEquals(activeDeclarativeManifest, result);
  }

  @Test
  void givenNoActiveManifestWhenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException {
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));
    assertThrows(ConfigNotFoundException.class,
        () -> configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

  @Test
  void whenGetActorDefinitionIdsWithActiveDeclarativeManifestThenReturnActorDefinitionIds() throws IOException {
    final UUID activeActorDefinitionId = UUID.randomUUID();
    final UUID anotherActorDefinitionId = UUID.randomUUID();
    givenActiveDeclarativeManifestWithActorDefinitionId(activeActorDefinitionId);
    givenActiveDeclarativeManifestWithActorDefinitionId(anotherActorDefinitionId);

    final List<UUID> results = configRepository.getActorDefinitionIdsWithActiveDeclarativeManifest().toList();

    assertEquals(2, results.size());
    assertEquals(results, List.of(activeActorDefinitionId, anotherActorDefinitionId));
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

    configRepository.createDeclarativeManifestAsActiveVersion(declarativeManifest, configInjection, connectorSpecification);

    final StandardSourceDefinition sourceDefinition = configRepository.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(List.of(configInjection), configRepository.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(declarativeManifest, configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(DataAccessException.class, () -> configRepository.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC)));
  }

  @Test
  void givenActorDefinitionIdMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> configRepository.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC)));
  }

  @Test
  void givenManifestMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> configRepository.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(ANOTHER_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(A_SPEC)));
  }

  @Test
  void givenSpecMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    assertThrows(IllegalArgumentException.class, () -> configRepository.createDeclarativeManifestAsActiveVersion(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withManifest(A_MANIFEST).withSpec(createSpec(A_SPEC)),
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(A_MANIFEST),
        MockData.connectorSpecification().withConnectionSpecification(ANOTHER_SPEC)));
  }

  @Test
  void whenSetDeclarativeSourceActiveVersionThenUpdateSourceDefinitionAndConfigInjectionAndActiveDeclarativeManifest() throws Exception {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION));
    final ActorDefinitionConfigInjection configInjection = MockData.actorDefinitionConfigInjection()
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withJsonToInject(A_MANIFEST);
    final ConnectorSpecification connectorSpecification = MockData.connectorSpecification().withConnectionSpecification(A_SPEC);

    configRepository.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID, A_VERSION, configInjection, connectorSpecification);

    final StandardSourceDefinition sourceDefinition = configRepository.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertEquals(connectorSpecification, configRepository.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId()).getSpec());
    assertEquals(List.of(configInjection), configRepository.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList());
    assertEquals(A_VERSION, configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).getVersion());
  }

  @Test
  void givenSourceDefinitionDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() {
    assertThrows(DataAccessException.class, () -> configRepository.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        MockData.actorDefinitionConfigInjection(),
        MockData.connectorSpecification()));
  }

  @Test
  void givenActiveDeclarativeManifestDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() throws Exception {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID);
    assertThrows(DataAccessException.class, () -> configRepository.setDeclarativeSourceActiveVersion(AN_ACTOR_DEFINITION_ID,
        A_VERSION,
        MockData.actorDefinitionConfigInjection().withActorDefinitionId(AN_ACTOR_DEFINITION_ID),
        MockData.connectorSpecification()));
  }

  void givenActiveDeclarativeManifestWithActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    final Long version = 4L;
    configRepository.insertActiveDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(actorDefinitionId).withVersion(version));
  }

  void givenSourceDefinition(final UUID sourceDefinitionId) throws JsonValidationException, IOException {
    final UUID workspaceId = UUID.randomUUID();
    configRepository.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0).withWorkspaceId(workspaceId));
    configRepository.writeCustomConnectorMetadata(
        MockData.customSourceDefinition().withSourceDefinitionId(sourceDefinitionId),
        MockData.actorDefinitionVersion().withActorDefinitionId(sourceDefinitionId),
        workspaceId,
        ScopeType.WORKSPACE);
  }

  JsonNode createSpec(final JsonNode connectionSpecification) {
    return new ObjectMapper().createObjectNode().set("connectionSpecification", connectionSpecification);
  }

}
