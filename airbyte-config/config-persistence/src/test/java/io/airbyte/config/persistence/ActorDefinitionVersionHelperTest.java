/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.version_overrides.LocalDefinitionVersionOverrideProvider;
import io.airbyte.config.persistence.version_overrides.OverrideTargetType;
import io.airbyte.featureflag.ConnectorVersionOverridesEnabled;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionVersionHelperTest {

  private LocalDefinitionVersionOverrideProvider mOverrideProvider;
  private FeatureFlagClient mFeatureFlagClient;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "0.2.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value",
          "key2", "value2")));

  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);
  private static final ActorDefinitionVersion OVERRIDDEN_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2);

  @BeforeEach
  void setup() {
    mOverrideProvider = mock(LocalDefinitionVersionOverrideProvider.class);
    when(mOverrideProvider.getOverride(any(), any(), any(), any())).thenReturn(Optional.empty());

    mFeatureFlagClient = mock(TestClient.class);
    when(mFeatureFlagClient.boolVariation(ConnectorVersionOverridesEnabled.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);

    actorDefinitionVersionHelper = new ActorDefinitionVersionHelper(mOverrideProvider, mFeatureFlagClient);
  }

  @Test
  void testGetSourceVersion() {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionWithActorOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetSourceVersionWithDisabledFeatureFlag() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    when(mFeatureFlagClient.boolVariation(ConnectorVersionOverridesEnabled.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(false);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionWithWorkspaceOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, OverrideTargetType.WORKSPACE, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetSourceVersionForWorkspace() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(expected, actual);
  }

  @Test
  void testGetSourceVersionForWorkspaceWithOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, OverrideTargetType.WORKSPACE, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersion() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(expected, actual);
  }

  @Test
  void testGetDestinationVersionWithActorOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionWithWorkspaceOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, OverrideTargetType.WORKSPACE, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspace() {
    final ActorDefinitionVersion expected = new ActorDefinitionVersion()
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withSpec(SPEC);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(expected, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspaceWithOverride() {
    when(mOverrideProvider.getOverride(ACTOR_DEFINITION_ID, WORKSPACE_ID, OverrideTargetType.WORKSPACE, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

}
