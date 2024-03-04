/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.persistence.MockData;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseBreakingChangeScopes;
import io.airbyte.featureflag.Workspace;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class DestinationServiceJooqImplTest extends BaseConfigDatabaseTest {

  private static final String UPGRADE_IMAGE_TAG = "0.0.2";
  private DestinationServiceJooqImpl destinationServiceJooqImpl;
  private FeatureFlagClient featureFlagClient;
  private SourceServiceJooqImpl sourceServiceJooqImpl;
  private ConnectionService connectionService;

  @BeforeEach
  void setup() {
    this.featureFlagClient = mock(TestClient.class);
    when(featureFlagClient.stringVariation(eq(HeartbeatMaxSecondsBetweenMessages.INSTANCE), any(SourceDefinition.class))).thenReturn("3600");

    final SecretsRepositoryReader secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final SecretsRepositoryWriter secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final SecretPersistenceConfigService secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    connectionService = mock(ConnectionService.class);
    this.destinationServiceJooqImpl = new DestinationServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService, connectionService);
    // We don't actually need this service in the test, we just use it for extra validating.
    // I'd take it out to keep it 'clean', but really this should be happening in a service
    // That handles both sources and destinations. They're in the same table that we modify,
    // So it's safer to have this to ensure we're modifying only the destination as expected.
    this.sourceServiceJooqImpl = new SourceServiceJooqImpl(database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService, connectionService);
    when(featureFlagClient.boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testScopedImpactAffectsBreakingChangeImpact(final boolean actorIsInBreakingChangeScope)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    when(featureFlagClient.boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(true);

    // Setup and get setup info
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();
    final SourceConnection source = jooqTestDbSetupHelper.getSource();
    final DestinationConnection destination = jooqTestDbSetupHelper.getDestination();
    final StandardDestinationDefinition destinationDefinition = jooqTestDbSetupHelper.getDestinationDefinition();

    // Create a new version of the destination, with a stream-scoped breaking change
    final ActorDefinitionBreakingChange streamScopedBreakingChange =
        MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
            .withScopedImpact(List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("affected_stream"))));
    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newDestinationVersion = Jsons.clone(jooqTestDbSetupHelper.getDestinationDefinitionVersion())
        .withVersionId(newVersionId).withDockerImageTag(UPGRADE_IMAGE_TAG);

    // Write new version
    // TODO: after uncoupling the transaction, this test will move to ApplyDefinitionsHelper.
    // When we do that we can mock `actorIsInBreakingChangeScope` instead of the further down
    // actorSyncsAnyListedStream
    when(connectionService.actorSyncsAnyListedStream(destination.getDestinationId(), List.of("affected_stream")))
        .thenReturn(actorIsInBreakingChangeScope);

    destinationServiceJooqImpl.writeConnectorMetadata(destinationDefinition, newDestinationVersion, List.of(streamScopedBreakingChange));
    verify(featureFlagClient).boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS));
    verify(connectionService).actorSyncsAnyListedStream(destination.getDestinationId(), List.of("affected_stream"));

    // Get the destination definition and actor versions after the upgrade
    final UUID destinationDefinitionDefaultVersionIdAfterUpgrade =
        destinationServiceJooqImpl.getStandardDestinationDefinition(destinationDefinition.getDestinationDefinitionId()).getDefaultVersionId();
    final UUID destinationDefaultVersionIdAfterUpgrade =
        destinationServiceJooqImpl.getDestinationConnection(destination.getDestinationId()).getDefaultVersionId();

    // The destination definition should always get the new version
    assertEquals(newVersionId, destinationDefinitionDefaultVersionIdAfterUpgrade);
    // The source actor's version should not get messed with
    assertEquals(jooqTestDbSetupHelper.getInitialSourceDefaultVersionId(),
        sourceServiceJooqImpl.getSourceConnection(source.getSourceId()).getDefaultVersionId());

    if (actorIsInBreakingChangeScope) {
      // Assert actor is held back
      assertEquals(jooqTestDbSetupHelper.getInitialDestinationDefaultVersionId(), destinationDefaultVersionIdAfterUpgrade);
    } else {
      // Assert actor is upgraded to the new version
      assertEquals(newVersionId, destinationDefaultVersionIdAfterUpgrade);
    }
    verifyNoMoreInteractions(featureFlagClient, connectionService);
  }

}
