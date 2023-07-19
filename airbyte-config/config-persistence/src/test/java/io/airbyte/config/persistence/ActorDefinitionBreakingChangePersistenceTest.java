/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionBreakingChangePersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();

    configRepository = spy(new ConfigRepository(database, mock(StandardSyncPersistence.class), MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));

    configRepository.writeStandardSourceDefinition(new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withName("Test Source"));
  }

  @Test
  void testReadAndWriteActorDefinitionBreakingChange() throws IOException {
    final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withVersion(new Version("1.0.0"))
        .withMessage("This is a breaking change")
        .withMigrationDocumentationUrl("https://docs.airbyte.com/migration")
        .withUpgradeDeadline("2025-01-21");

    final List<ActorDefinitionBreakingChange> prevBreakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    assertEquals(0, prevBreakingChanges.size());

    configRepository.writeActorDefinitionBreakingChange(breakingChange);
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID);
    assertEquals(1, breakingChanges.size());
    assertEquals(breakingChange, breakingChanges.get(0));

    // unique key prevents duplicates
    assertThrows(DataAccessException.class, () -> configRepository.writeActorDefinitionBreakingChange(breakingChange));
    assertEquals(1, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID).size());
  }

}
