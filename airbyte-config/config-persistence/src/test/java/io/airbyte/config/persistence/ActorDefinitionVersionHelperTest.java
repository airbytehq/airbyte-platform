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
import io.airbyte.config.ActorType;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.version_overrides.DefinitionVersionOverrideProvider;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.TestClient;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ActorDefinitionVersionHelperTest {

  private DefinitionVersionOverrideProvider mOverrideProvider;
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

  private static final UUID DEFAULT_VERSION_ID = UUID.randomUUID();

  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
      .withVersionId(DEFAULT_VERSION_ID)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);
  private static final ActorDefinitionVersion OVERRIDDEN_VERSION = new ActorDefinitionVersion()
      .withVersionId(UUID.randomUUID())
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2);

  @BeforeEach
  void setup() throws ConfigNotFoundException, IOException {
    mOverrideProvider = mock(DefinitionVersionOverrideProvider.class);
    when(mOverrideProvider.getOverride(any(), any(), any(), any(), any())).thenReturn(Optional.empty());

    mFeatureFlagClient = mock(TestClient.class);

    mConfigRepository = mock(ConfigRepository.class);
    when(mConfigRepository.getActorDefinitionVersion(DEFAULT_VERSION_ID)).thenReturn(DEFAULT_VERSION);
    actorDefinitionVersionHelper =
        new ActorDefinitionVersionHelper(mConfigRepository, mOverrideProvider, mFeatureFlagClient);
  }

  @Test
  void testGetSourceVersion() throws ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionWithOverride() throws ConfigNotFoundException, IOException {
    when(mOverrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetSourceVersionForWorkspace() throws ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetSourceVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException {
    when(mOverrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, null, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersion() throws ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionWithOverride() throws ConfigNotFoundException, IOException {
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID, ACTOR_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspace() throws ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, actual);
  }

  @Test
  void testGetDestinationVersionForWorkspaceWithOverride() throws ConfigNotFoundException, IOException {
    when(mOverrideProvider.getOverride(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, null, DEFAULT_VERSION))
        .thenReturn(Optional.of(OVERRIDDEN_VERSION));

    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(DEFAULT_VERSION_ID);

    final ActorDefinitionVersion actual = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(OVERRIDDEN_VERSION, actual);
  }

  @Test
  void testGetDefaultSourceVersion() throws ConfigNotFoundException, IOException {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultDestinationVersion() throws ConfigNotFoundException, IOException {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
        .withDefaultVersionId(ACTOR_DEFINITION_VERSION_ID);

    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_VERSION_ID)).thenReturn(DEFAULT_VERSION);

    final ActorDefinitionVersion result = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID);
    assertEquals(DEFAULT_VERSION, result);
  }

  @Test
  void testGetDefaultVersionWithNoDefaultThrows() {
    final StandardSourceDefinition sourceDefinition = new StandardSourceDefinition()
        .withSourceDefinitionId(ACTOR_DEFINITION_ID);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("has no default version"));
  }

  @Test
  void testGetDefaultDestinationVersionWithNoDefaultThrows() {
    final StandardDestinationDefinition destinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(ACTOR_DEFINITION_ID);

    final RuntimeException exception =
        assertThrows(RuntimeException.class, () -> actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, WORKSPACE_ID));
    assertTrue(exception.getMessage().contains("has no default version"));
  }

  @ParameterizedTest
  @CsvSource({"alpha,generally_available,true", "beta,generally_available,true", "generally_available,generally_available,false", "alpha,beta,true"})
  void testHasAlphaOrBeta(final String sourceReleaseStageStr, final String destinationReleaseStageStr, final boolean expected) {
    final ActorDefinitionVersion sourceDefVersion = new ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(sourceReleaseStageStr));
    final ActorDefinitionVersion destDefVersion = new ActorDefinitionVersion().withReleaseStage(ReleaseStage.fromValue(destinationReleaseStageStr));
    assertEquals(expected, ActorDefinitionVersionHelper.hasAlphaOrBetaVersion(List.of(sourceDefVersion, destDefVersion)));
  }

}
