package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.config.ActiveDeclarativeManifest;
import java.io.IOException;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ActiveDeclarativeManifestTest extends BaseConfigDatabaseTest {

  private static final UUID AN_ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final Long A_VERSION = 43L;
  private static final Long ANOTHER_VERSION = 3993L;

  private ConfigRepository configRepository;

  @BeforeEach
  void beforeEach() throws Exception {
    truncateAllTables();
    configRepository = new ConfigRepository(database);
  }

  @Test
  void givenActorDefinitionIdAlreadyExistWhenUpsertActiveDeclarativeManifestThenThrowException() throws IOException {
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));
    configRepository.insertDeclarativeManifest(MockData.declarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION));
    configRepository.upsertActiveDeclarativeManifest(
        activeDeclarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION));

    assertThrows(DataAccessException.class, () -> configRepository.upsertActiveDeclarativeManifest(
        activeDeclarativeManifest().withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION)));
  }

  static ActiveDeclarativeManifest activeDeclarativeManifest() {
    return MockData.activeDeclarativeManifest();
  }

}
