/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.TestClient;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionServiceJooqImplTest extends BaseConfigDatabaseTest {

  private SourceService sourceService;
  private ActorDefinitionServiceJooqImpl actorDefinitionService;

  @BeforeEach
  void setUp() {
    this.actorDefinitionService = new ActorDefinitionServiceJooqImpl(database);

    final FeatureFlagClient featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any())).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final ConnectionService connectionService = mock(ConnectionService.class);
    final ActorDefinitionVersionUpdater actorDefinitionVersionUpdater =
        new ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService);
    this.sourceService = new SourceServiceJooqImpl(database, featureFlagClient, secretsRepositoryReader, secretsRepositoryWriter,
        secretPersistenceConfigService, connectionService, actorDefinitionVersionUpdater);
  }

  @Test
  void testSetActorDefaultVersions() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();

    final UUID actorId = jooqTestDbSetupHelper.getSource().getSourceId();
    final UUID otherActorId = UUID.randomUUID();
    final SourceConnection otherSource = Jsons.clone(jooqTestDbSetupHelper.getSource()).withSourceId(otherActorId);
    sourceService.writeSourceConnectionNoSecrets(otherSource);

    final ActorDefinitionVersion newVersion =
        Jsons.clone(jooqTestDbSetupHelper.getSourceDefinitionVersion()).withVersionId(UUID.randomUUID()).withDockerImageTag("5.0.0");
    actorDefinitionService.writeActorDefinitionVersion(newVersion);

    actorDefinitionService.setActorDefaultVersions(List.of(actorId), newVersion.getVersionId());

    final Set<UUID> actorsOnNewDefaultVersion = actorDefinitionService.getActorsWithDefaultVersionId(newVersion.getVersionId());
    assertEquals(Set.of(actorId), actorsOnNewDefaultVersion);

    final Set<UUID> actorsOnOldDefaultVersion =
        actorDefinitionService.getActorsWithDefaultVersionId(jooqTestDbSetupHelper.getInitialSourceDefaultVersionId());
    assertEquals(Set.of(otherActorId), actorsOnOldDefaultVersion);
  }

  @Test
  void testGetActorsWithDefaultVersionId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();

    final UUID actorId = jooqTestDbSetupHelper.getSource().getSourceId();
    final Set<UUID> actorIds = actorDefinitionService.getActorsWithDefaultVersionId(jooqTestDbSetupHelper.getInitialSourceDefaultVersionId());
    assertEquals(Set.of(actorId), actorIds);
  }

  @Test
  void updateActorDefinitionDefaultVersionId() throws JsonValidationException, ConfigNotFoundException, IOException {
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();

    final UUID actorDefinitionId = jooqTestDbSetupHelper.getSourceDefinition().getSourceDefinitionId();
    final StandardSourceDefinition sourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId);
    assertEquals(sourceDefinition.getDefaultVersionId(), jooqTestDbSetupHelper.getInitialSourceDefaultVersionId());

    final ActorDefinitionVersion newVersion =
        Jsons.clone(jooqTestDbSetupHelper.getSourceDefinitionVersion()).withVersionId(UUID.randomUUID()).withDockerImageTag("5.0.0");
    actorDefinitionService.writeActorDefinitionVersion(newVersion);

    actorDefinitionService.updateActorDefinitionDefaultVersionId(actorDefinitionId, newVersion.getVersionId());

    final StandardSourceDefinition updatedSourceDefinition = sourceService.getStandardSourceDefinition(actorDefinitionId);
    assertEquals(updatedSourceDefinition.getDefaultVersionId(), newVersion.getVersionId());
  }

}
