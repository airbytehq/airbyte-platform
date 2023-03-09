/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.airbyte.config.ActiveDeclarativeManifest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActiveDeclarativeManifestPersistenceTest extends BaseConfigDatabaseTest {

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database, MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
  }

  @Test
  void whenGetActorDefinitionIdsWithActiveDeclarativeManifestThenReturnActorDefinitionIds() throws IOException, JsonValidationException {
    final UUID activeActorDefinitionId = UUID.randomUUID();
    final UUID anotherActorDefinitionId = UUID.randomUUID();
    givenActiveDeclarativeManifestWithActorDefinitionId(activeActorDefinitionId);
    givenActiveDeclarativeManifestWithActorDefinitionId(anotherActorDefinitionId);

    final List<UUID> results = configRepository.getActorDefinitionIdsWithActiveDeclarativeManifest().toList();

    assertEquals(2, results.size());
    assertEquals(results, List.of(activeActorDefinitionId, anotherActorDefinitionId));
  }

  static ActiveDeclarativeManifest activeDeclarativeManifest() {
    return MockData.activeDeclarativeManifest();
  }

  void givenActiveDeclarativeManifestWithActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    final Long version = 4L;
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(actorDefinitionId).withVersion(version));
    configRepository
        .upsertActiveDeclarativeManifest(activeDeclarativeManifest().withActorDefinitionId(actorDefinitionId).withVersion(version));
  }

}
