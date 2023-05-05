/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import io.airbyte.featureflag.UseActorDefinitionVersionTableDefaults;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionVersionHelperTest {

  private LocalDefinitionVersionOverrideProvider mOverrideProvider;
  private FeatureFlagClient mFeatureFlagClient;
  private ConfigRepository mConfigRepository;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID();
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

    mConfigRepository = mock(ConfigRepository.class);
    actorDefinitionVersionHelper = new ActorDefinitionVersionHelper(mConfigRepository, mOverrideProvider, mFeatureFlagClient);
  }

  @Test
  void testGetSourceVersion() throws ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerRepository(DEFAULT_VERSION.getDockerRepository())
        .withDockerImageTag(DEFAULT_VERSION.getDockerImageTag())
        .withSpec(DEFAULT_VERSION.getSpec());

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionWithActorOverride() throws ConfigNotFoundException, IOException {
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
  void testGetSourceVersionWithDisabledFeatureFlag() throws ConfigNotFoundException, IOException {
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
  void testGetSourceVersionWithWorkspaceOverride() throws ConfigNotFoundException, IOException {
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
  void testGetSourceVersionForWorkspace() throws ConfigNotFoundException, IOException {
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
  void testGetSourceVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException {
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
  void testGetDestinationVersion() throws ConfigNotFoundException, IOException {
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
  void testGetDestinationVersionWithActorOverride() throws ConfigNotFoundException, IOException {
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
  void testGetDestinationVersionWithWorkspaceOverride() throws ConfigNotFoundException, IOException {
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
  void testGetDestinationVersionForWorkspace() throws ConfigNotFoundException, IOException {
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
  void testGetDestinationVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException {
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

  @Test
  void testGetDefaultSourceVersionFromDb() throws ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorDefinitionVersionTableDefaults.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultDestinationVersionFromDb() throws ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorDefinitionVersionTableDefaults.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultVersionFromDbWithNoDefaultThrows() {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorDefinitionVersionTableDefaults.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("has no default version"));
  }

  @Test
  void testGetDefaultDestinationVersionFromDbWithNoDefaultThrows() {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    when(mFeatureFlagClient.boolVariation(UseActorDefinitionVersionTableDefaults.INSTANCE, new Workspace(WORKSPACE_ID))).thenReturn(true);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("has no default version"));
  }

}
