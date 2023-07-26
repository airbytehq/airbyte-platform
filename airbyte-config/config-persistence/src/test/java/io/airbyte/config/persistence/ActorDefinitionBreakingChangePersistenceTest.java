/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionBreakingChangePersistenceTest extends BaseConfigDatabaseTest {

  private static final UUID ACTOR_DEFINITION_ID_1 = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_ID_2 = UUID.randomUUID();

  private ConfigRepository configRepository;

  final ActorDefinitionBreakingChange breakingChange = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration")
      .withUpgradeDeadline("2025-01-21");
  final ActorDefinitionBreakingChange breakingChange2 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("2.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration")
      .withUpgradeDeadline("2025-01-21");
  final ActorDefinitionBreakingChange breakingChange3 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_2)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration")
      .withUpgradeDeadline("2025-01-21");

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();

    configRepository = spy(new ConfigRepository(database, mock(StandardSyncPersistence.class), MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));

    configRepository.writeStandardSourceDefinition(new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID_1)
        .withName("Test Source"));
    configRepository.writeStandardDestinationDefinition(new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID_2)
        .withName("Test Destination"));
  }

  @Test
  void testReadAndWriteActorDefinitionBreakingChange() throws IOException {
    final List<ActorDefinitionBreakingChange> prevBreakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(0, prevBreakingChanges.size());

    configRepository.writeActorDefinitionBreakingChanges(List.of(breakingChange));
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(1, breakingChanges.size());
    assertEquals(breakingChange, breakingChanges.get(0));
  }

  @Test
  void testUpdateActorDefinitionBreakingChange() throws IOException {
    final List<ActorDefinitionBreakingChange> prevBreakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(0, prevBreakingChanges.size());

    configRepository.writeActorDefinitionBreakingChanges(List.of(breakingChange));
    List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(1, breakingChanges.size());
    assertEquals(breakingChange, breakingChanges.get(0));

    // Update breaking change
    final ActorDefinitionBreakingChange updatedBreakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(breakingChange.getActorDefinitionId())
        .withVersion(breakingChange.getVersion())
        .withMessage("Updated message")
        .withUpgradeDeadline("2025-01-01")
        .withMigrationDocumentationUrl(breakingChange.getMigrationDocumentationUrl());
    configRepository.writeActorDefinitionBreakingChanges(List.of(updatedBreakingChange));

    // Check updated breaking change
    breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(1, breakingChanges.size());
    assertEquals(updatedBreakingChange, breakingChanges.get(0));
  }

  @Test
  void testWriteMultipleActorDefinitionBreakingChanges() throws IOException {
    assertEquals(0, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1).size());
    assertEquals(0, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_2).size());

    configRepository.writeActorDefinitionBreakingChanges(List.of(breakingChange, breakingChange2, breakingChange3));

    final List<ActorDefinitionBreakingChange> breakingChangesForId1 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(2, breakingChangesForId1.size());
    assertEquals(breakingChange, breakingChangesForId1.get(0));
    assertEquals(breakingChange2, breakingChangesForId1.get(1));

    final List<ActorDefinitionBreakingChange> breakingChangesForId2 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_2);
    assertEquals(1, breakingChangesForId2.size());
    assertEquals(breakingChange3, breakingChangesForId2.get(0));
  }

}
