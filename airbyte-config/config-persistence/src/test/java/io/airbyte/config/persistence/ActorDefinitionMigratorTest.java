/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionMigrator.ConnectorInfo;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionMigratorTest extends BaseConfigDatabaseTest {

  public static final String DEFAULT_PROTOCOL_VERSION = "0.2.0";

  private ActorDefinitionMigrator migrator;
  private ConfigRepository configRepository;
  private final FeatureFlagClient featureFlagClient = mock(TestClient.class);

  @BeforeEach
  void setup() throws SQLException {
    truncateAllTables();

    configRepository = new ConfigRepository(database, null, MockData.MAX_SECONDS_BETWEEN_MESSAGE_SUPPLIER);
    migrator = new ActorDefinitionMigrator(configRepository, featureFlagClient);
  }

  private void writeSource(final StandardSourceDefinition source) throws Exception {
    configRepository.writeStandardSourceDefinition(source);
  }

  @Test
  void testGetConnectorRepositoryToInfoMap() throws Exception {
    final String connectorRepository = "airbyte/duplicated-connector";
    final String oldVersion = "0.1.10";
    final String newVersion = DEFAULT_PROTOCOL_VERSION;
    final StandardSourceDefinition source1 = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source-1");
    final ActorDefinitionVersion source1ActorDefVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(source1.getSourceDefinitionId())
        .withDockerRepository(connectorRepository)
        .withDockerImageTag(oldVersion);
    final StandardSourceDefinition source2 = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source-2");
    final ActorDefinitionVersion source2ActorDefVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(source2.getSourceDefinitionId())
        .withDockerRepository(connectorRepository)
        .withDockerImageTag(newVersion);

    final String customConnectorRepository = "airbyte/custom";
    final StandardSourceDefinition customSource = new StandardSourceDefinition()
        .withSourceDefinitionId(UUID.randomUUID())
        .withName("source-3")
        .withCustom(true);
    final ActorDefinitionVersion customSourceActorDefVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(customSource.getSourceDefinitionId())
        .withDockerRepository(customConnectorRepository)
        .withDockerImageTag(newVersion)
        .withReleaseStage(ReleaseStage.CUSTOM);

    configRepository.writeSourceDefinitionAndDefaultVersion(source1, source1ActorDefVersion);
    configRepository.writeSourceDefinitionAndDefaultVersion(source2, source2ActorDefVersion);
    configRepository.writeSourceDefinitionAndDefaultVersion(customSource, customSourceActorDefVersion);

    final Map<String, ConnectorInfo> result = migrator.getConnectorRepositoryToInfoMap();
    // when there are duplicated connector definitions, the one with the latest version should be
    // retrieved
    assertEquals(newVersion, result.get(connectorRepository).dockerImageTag);
    // custom connectors are excluded
    assertNull(result.get(customConnectorRepository));
  }

  @Test
  void testUpdateIsAvailable() {
    assertTrue(ActorDefinitionMigrator.updateIsAvailable("0.1.99", DEFAULT_PROTOCOL_VERSION));
    assertFalse(ActorDefinitionMigrator.updateIsAvailable("invalid_version", "0.1.2"));
  }

  @Test
  void testUpdateIsPatchOnly() {
    assertFalse(ActorDefinitionMigrator.updateIsPatchOnly("0.1.99", DEFAULT_PROTOCOL_VERSION));
    assertFalse(ActorDefinitionMigrator.updateIsPatchOnly("invalid_version", "0.3.1"));
    assertTrue(ActorDefinitionMigrator.updateIsPatchOnly("0.1.0", "0.1.3"));
  }

  @Test
  void testActorDefinitionReleaseDate() throws Exception {
    final UUID definitionId = UUID.randomUUID();

    // when the record does not exist, it is inserted
    final StandardSourceDefinition sourceDef = new StandardSourceDefinition()
        .withSourceDefinitionId(definitionId)
        .withName("random-name")
        .withTombstone(false)
        .withMaxSecondsBetweenMessages(MockData.DEFAULT_MAX_SECONDS_BETWEEN_MESSAGES);
    writeSource(sourceDef);
    assertEquals(sourceDef, configRepository.getStandardSourceDefinition(sourceDef.getSourceDefinitionId()));
    // TODO assertions on ADVs
  }

}
