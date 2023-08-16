/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
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

  private static final StandardSourceDefinition SOURCE_DEFINITION = new StandardSourceDefinition()
      .withName("Test Source")
      .withSourceDefinitionId(ACTOR_DEFINITION_ID_1);

  private static final StandardDestinationDefinition DESTINATION_DEFINITION = new StandardDestinationDefinition()
      .withName("Test Destination")
      .withDestinationDefinitionId(ACTOR_DEFINITION_ID_2);

  private static final ActorDefinitionBreakingChange BREAKING_CHANGE = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is an older breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#1.0.0")
      .withUpgradeDeadline("2025-01-21");
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_2 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("2.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#2.0.0")
      .withUpgradeDeadline("2025-02-21");
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_3 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("3.0.0"))
      .withMessage("This is another breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#3.0.0")
      .withUpgradeDeadline("2025-03-21");
  private static final ActorDefinitionBreakingChange BREAKING_CHANGE_4 = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
      .withVersion(new Version("4.0.0"))
      .withMessage("This is some future breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration#4.0.0")
      .withUpgradeDeadline("2025-03-21");
  private static final ActorDefinitionBreakingChange OTHER_CONNECTOR_BREAKING_CHANGE = new ActorDefinitionBreakingChange()
      .withActorDefinitionId(ACTOR_DEFINITION_ID_2)
      .withVersion(new Version("1.0.0"))
      .withMessage("This is a breaking change")
      .withMigrationDocumentationUrl("https://docs.airbyte.com/migration-2#1.0.0")
      .withUpgradeDeadline("2025-01-21");

  private ConfigRepository configRepository;

  @BeforeEach
  void setup() throws SQLException, JsonValidationException, IOException {
    truncateAllTables();

    configRepository = spy(new ConfigRepository(database, mock(StandardSyncPersistence.class), MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER));

    configRepository.writeStandardSourceDefinition(SOURCE_DEFINITION);
    configRepository.writeStandardDestinationDefinition(DESTINATION_DEFINITION);
  }

  @Test
  void testReadAndWriteActorDefinitionBreakingChange() throws IOException {
    final List<ActorDefinitionBreakingChange> prevBreakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(0, prevBreakingChanges.size());

    configRepository.writeActorDefinitionBreakingChanges(List.of(BREAKING_CHANGE));
    final List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(1, breakingChanges.size());
    assertEquals(BREAKING_CHANGE, breakingChanges.get(0));
  }

  @Test
  void testUpdateActorDefinitionBreakingChange() throws IOException {
    final List<ActorDefinitionBreakingChange> prevBreakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(0, prevBreakingChanges.size());

    configRepository.writeActorDefinitionBreakingChanges(List.of(BREAKING_CHANGE));
    List<ActorDefinitionBreakingChange> breakingChanges = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(1, breakingChanges.size());
    assertEquals(BREAKING_CHANGE, breakingChanges.get(0));

    // Update breaking change
    final ActorDefinitionBreakingChange updatedBreakingChange = new ActorDefinitionBreakingChange()
        .withActorDefinitionId(BREAKING_CHANGE.getActorDefinitionId())
        .withVersion(BREAKING_CHANGE.getVersion())
        .withMessage("Updated message")
        .withUpgradeDeadline("2025-01-01")
        .withMigrationDocumentationUrl(BREAKING_CHANGE.getMigrationDocumentationUrl());
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

    configRepository.writeActorDefinitionBreakingChanges(List.of(BREAKING_CHANGE, BREAKING_CHANGE_2, OTHER_CONNECTOR_BREAKING_CHANGE));

    final List<ActorDefinitionBreakingChange> breakingChangesForId1 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1);
    assertEquals(2, breakingChangesForId1.size());
    assertEquals(BREAKING_CHANGE, breakingChangesForId1.get(0));
    assertEquals(BREAKING_CHANGE_2, breakingChangesForId1.get(1));

    final List<ActorDefinitionBreakingChange> breakingChangesForId2 = configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_2);
    assertEquals(1, breakingChangesForId2.size());
    assertEquals(OTHER_CONNECTOR_BREAKING_CHANGE, breakingChangesForId2.get(0));
  }

  @Test
  void testListBreakingChangesForVersion() throws IOException {
    assertEquals(0, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1).size());
    configRepository.writeActorDefinitionBreakingChanges(List.of(BREAKING_CHANGE, BREAKING_CHANGE_2, BREAKING_CHANGE_3, BREAKING_CHANGE_4));

    final ActorDefinitionVersion ADV_4_0_0 = createActorDefinitionVersion("4.0.0");
    configRepository.writeSourceDefinitionAndDefaultVersion(SOURCE_DEFINITION, ADV_4_0_0);

    // no breaking changes for latest default
    assertEquals(4, configRepository.listBreakingChangesForActorDefinition(ACTOR_DEFINITION_ID_1).size());
    assertEquals(0, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_4_0_0).size());

    // should see future breaking changes for 2.0.0
    final ActorDefinitionVersion ADV_2_0_0 = createActorDefinitionVersion("2.0.0");
    assertEquals(2, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0).size());
    assertEquals(List.of(BREAKING_CHANGE_3, BREAKING_CHANGE_4), configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0));

    // move back default version for Actor Definition to 3.0.0, should stop seeing "rolled back"
    // breaking changes
    final ActorDefinitionVersion ADV_3_0_0 = createActorDefinitionVersion("3.0.0");
    configRepository.writeSourceDefinitionAndDefaultVersion(SOURCE_DEFINITION, ADV_3_0_0);
    assertEquals(1, configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0).size());
    assertEquals(List.of(BREAKING_CHANGE_3), configRepository.listBreakingChangesForActorDefinitionVersion(ADV_2_0_0));
  }

  ActorDefinitionVersion createActorDefinitionVersion(final String version) {
    return new ActorDefinitionVersion()
        .withActorDefinitionId(ACTOR_DEFINITION_ID_1)
        .withDockerRepository("some-repo")
        .withDockerImageTag(version);
  }

}
