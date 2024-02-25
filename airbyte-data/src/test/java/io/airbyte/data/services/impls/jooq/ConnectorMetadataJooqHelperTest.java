/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.MockData;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.UseBreakingChangeScopes;
import io.airbyte.featureflag.Workspace;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ConnectorMetadataJooqHelperTest extends BaseConfigDatabaseTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String UPGRADE_IMAGE_TAG = "1.0.0";
  private final FeatureFlagClient featureFlagClient;
  private final ConnectionService connectionService;
  private final ConnectorMetadataJooqHelper connectorMetadataJooqHelper;

  public ConnectorMetadataJooqHelperTest() {
    this.featureFlagClient = mock(TestClient.class);
    this.connectionService = mock(ConnectionService.class);
    this.connectorMetadataJooqHelper = new ConnectorMetadataJooqHelper(featureFlagClient, connectionService);
  }

  private static Stream<Arguments> getBreakingChangesForUpgradeMethodSource() {
    return Stream.of(
        // Version increases
        Arguments.of("0.0.1", "2.0.0", List.of("1.0.0", "2.0.0")),
        Arguments.of("1.0.0", "1.0.1", List.of()),
        Arguments.of("1.0.0", "1.1.0", List.of()),
        Arguments.of("1.0.1", "1.1.0", List.of()),
        Arguments.of("1.0.0", "2.0.1", List.of("2.0.0")),
        Arguments.of("1.0.1", "2.0.0", List.of("2.0.0")),
        Arguments.of("1.0.0", "2.0.1", List.of("2.0.0")),
        Arguments.of("1.0.1", "2.0.1", List.of("2.0.0")),
        Arguments.of("2.0.0", "2.0.0", List.of()),
        // Version decreases - should never have breaking changes
        Arguments.of("2.0.0", "0.0.1", List.of()),
        Arguments.of("1.0.1", "1.0.0", List.of()),
        Arguments.of("1.1.0", "1.0.0", List.of()),
        Arguments.of("1.1.0", "1.0.1", List.of()),
        Arguments.of("2.0.0", "1.0.0", List.of()),
        Arguments.of("2.0.0", "1.0.1", List.of()),
        Arguments.of("2.0.1", "1.0.0", List.of()),
        Arguments.of("2.0.1", "1.0.1", List.of()),
        Arguments.of("2.0.0", "2.0.0", List.of()));
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  void testGetBreakingChangesForUpgradeWithActorDefBreakingChanges(final String initialImageTag,
                                                                   final String upgradeImageTag,
                                                                   final List<String> expectedBreakingChangeVersions) {
    final List<Version> expectedBreakingChangeVersionsForUpgrade = expectedBreakingChangeVersions.stream().map(Version::new).toList();
    final List<ActorDefinitionBreakingChange> breakingChangesForDef = List.of(
        new ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withVersion(new Version("1.0.0"))
            .withMessage("Breaking change 1")
            .withUpgradeDeadline("2021-01-01")
            .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/1.0.0"),
        new ActorDefinitionBreakingChange()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withVersion(new Version("2.0.0"))
            .withMessage("Breaking change 2")
            .withUpgradeDeadline("2020-08-09")
            .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/2.0.0"));
    final List<ActorDefinitionBreakingChange> breakingChangesForUpgrade =
        ConnectorMetadataJooqHelper.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef);
    final List<Version> actualBreakingChangeVersionsForUpgrade =
        breakingChangesForUpgrade.stream().map(ActorDefinitionBreakingChange::getVersion).toList();
    assertEquals(expectedBreakingChangeVersionsForUpgrade.size(), actualBreakingChangeVersionsForUpgrade.size());
    assertTrue(actualBreakingChangeVersionsForUpgrade.containsAll(expectedBreakingChangeVersionsForUpgrade));
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  void testGetBreakingChangesForUpgradeWithNoActorDefinitionBreakingChanges(final String initialImageTag,
                                                                            final String upgradeImageTag,
                                                                            final List<String> expectedBreakingChangeVersions) {
    final List<ActorDefinitionBreakingChange> breakingChangesForDef = List.of();
    assertTrue(ConnectorMetadataJooqHelper.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetActorsForNonBreakingUpgrade(final boolean useBreakingChangeScopes)
      throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    when(featureFlagClient.boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(useBreakingChangeScopes);

    // Setup and get setup info
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();
    final ActorDefinitionVersion actorDefinitionVersion = jooqTestDbSetupHelper.getSourceDefinitionVersion();
    final UUID actorIdOnInitialVersion = jooqTestDbSetupHelper.getSource().getSourceId();

    // Create a new version of the actor definition
    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newActorDefinitionVersion = Jsons.clone(actorDefinitionVersion)
        .withVersionId(newVersionId).withDockerImageTag(UPGRADE_IMAGE_TAG);

    final Set<UUID> actorsToUpgrade =
        database.query(ctx -> connectorMetadataJooqHelper.getActorsToUpgrade(actorDefinitionVersion, newActorDefinitionVersion, List.of(), ctx));

    // All actors should get upgraded
    assertEquals(Set.of(actorIdOnInitialVersion), actorsToUpgrade);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetActorsForBreakingUpgrade(final Boolean useBreakingChangeScopes)
      throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    when(featureFlagClient.boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(useBreakingChangeScopes);

    // Setup and get setup info
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();
    final StandardSourceDefinition actorDefinition = jooqTestDbSetupHelper.getSourceDefinition();
    final ActorDefinitionVersion actorDefinitionVersion = jooqTestDbSetupHelper.getSourceDefinitionVersion();
    final SourceConnection actorNotSyncingAffectedStream = jooqTestDbSetupHelper.getSource();

    // Create a new version of the destination, with a stream-scoped breaking change
    final ActorDefinitionBreakingChange streamScopedBreakingChange =
        MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(actorDefinitionVersion.getActorDefinitionId())
            .withScopedImpact(List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("affected_stream"))));
    final UUID newVersionId = UUID.randomUUID();
    final ActorDefinitionVersion newActorDefinitionVersion = Jsons.clone(actorDefinitionVersion)
        .withVersionId(newVersionId).withDockerImageTag(UPGRADE_IMAGE_TAG);

    // Create a second actor that syncs an affected stream
    final SourceConnection actorSyncingAffectedStream = jooqTestDbSetupHelper.createActorForActorDefinition(actorDefinition);
    when(connectionService.actorSyncsAnyListedStream(actorSyncingAffectedStream.getSourceId(), List.of("affected_stream"))).thenReturn(true);

    final Set<UUID> actorsToUpgrade = database.query(ctx -> connectorMetadataJooqHelper.getActorsToUpgrade(actorDefinitionVersion,
        newActorDefinitionVersion, List.of(streamScopedBreakingChange), ctx));

    if (useBreakingChangeScopes) {
      // Unaffected actors will be upgraded
      assertEquals(Set.of(actorNotSyncingAffectedStream.getSourceId()), actorsToUpgrade);
    } else {
      // No actors will be upgraded
      assertEquals(Set.of(), actorsToUpgrade);
    }

  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetActorsAffectedByBreakingChange(final Boolean useBreakingChangeScopes)
      throws JsonValidationException, ConfigNotFoundException, IOException, SQLException {
    when(featureFlagClient.boolVariation(UseBreakingChangeScopes.INSTANCE, new Workspace(ANONYMOUS))).thenReturn(useBreakingChangeScopes);

    // Setup and get setup info
    final JooqTestDbSetupHelper jooqTestDbSetupHelper = new JooqTestDbSetupHelper();
    jooqTestDbSetupHelper.setupForVersionUpgradeTest();
    final StandardSourceDefinition actorDefinition = jooqTestDbSetupHelper.getSourceDefinition();
    final ActorDefinitionVersion actorDefinitionVersion = jooqTestDbSetupHelper.getSourceDefinitionVersion();
    final UUID actorNotSyncingAffectedStreamId = jooqTestDbSetupHelper.getSource().getSourceId();

    // Create a new version of the destination, with a stream-scoped breaking change
    final ActorDefinitionBreakingChange streamScopedBreakingChange =
        MockData.actorDefinitionBreakingChange(UPGRADE_IMAGE_TAG).withActorDefinitionId(actorDefinitionVersion.getActorDefinitionId())
            .withScopedImpact(List.of(new BreakingChangeScope().withScopeType(ScopeType.STREAM).withImpactedScopes(List.of("affected_stream"))));

    // Create a second actor that syncs an affected stream
    final SourceConnection actorSyncingAffectedStream = jooqTestDbSetupHelper.createActorForActorDefinition(actorDefinition);
    final UUID actorSyncingAffectedStreamId = actorSyncingAffectedStream.getSourceId();
    when(connectionService.actorSyncsAnyListedStream(actorSyncingAffectedStreamId, List.of("affected_stream"))).thenReturn(true);

    final Set<UUID> actorsAffectedByBreakingChange = connectorMetadataJooqHelper.getActorsAffectedByBreakingChange(
        Set.of(actorSyncingAffectedStream.getSourceId(), actorNotSyncingAffectedStreamId), streamScopedBreakingChange);

    if (useBreakingChangeScopes) {
      // Affected actors depend on scopes
      assertEquals(Set.of(actorSyncingAffectedStreamId), actorsAffectedByBreakingChange);
    } else {
      // All actors are affected by breaking change
      assertEquals(Set.of(actorSyncingAffectedStreamId, actorNotSyncingAffectedStreamId), actorsAffectedByBreakingChange);
    }
  }

}
