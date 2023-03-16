/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.config.DeclarativeManifest;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeclarativeManifestPersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID AN_ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final Long A_VERSION = 1L;
  private static final Long ANOTHER_VERSION = 2L;
  private static final JsonNode A_MANIFEST;
  private static final JsonNode A_SPEC;

  static {
    try {
      A_MANIFEST = new ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}");
      A_SPEC = new ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}");
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database, MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
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
  void givenNoDeclarativeManifestMatchingWhenGetDeclarativeManifestByActorDefinitionIdAndVersionThenThrowException()
      throws IOException, ConfigNotFoundException {
    assertThrows(ConfigNotFoundException.class,
        () -> configRepository.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION));
  }

  @Test
  void whenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException, ConfigNotFoundException {
    final DeclarativeManifest declarativeManifest =
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION);
    configRepository.insertDeclarativeManifest(declarativeManifest);
    configRepository.insertDeclarativeManifest(
        MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION));
    configRepository.upsertActiveDeclarativeManifest(
        MockData.activeDeclarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));

    final DeclarativeManifest result = configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID);

    assertEquals(declarativeManifest, result);
  }

  @Test
  void givenNoActiveManifestWhenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() throws IOException {
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));
    assertThrows(ConfigNotFoundException.class,
        () -> configRepository.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID));
  }

}
