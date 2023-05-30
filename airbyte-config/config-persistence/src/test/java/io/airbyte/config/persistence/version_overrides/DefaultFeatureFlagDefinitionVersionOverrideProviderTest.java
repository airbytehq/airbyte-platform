/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.GcsBucketSpecFetcher;
import io.airbyte.featureflag.ConnectorVersionOverride;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.Workspace;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultFeatureFlagDefinitionVersionOverrideProviderTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "2.0.2";
  private static final String DOCKER_IMG_FORMAT = "%s:%s";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "theSpec", "goesHere")));
  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);
  private static final ActorDefinitionVersion OVERRIDE_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2);

  private DefaultFeatureFlagDefinitionVersionOverrideProvider overrideProvider;
  private GcsBucketSpecFetcher mGcsBucketSpecFetcher;
  private ConfigRepository mConfigRepository;
  private FeatureFlagClient mFeatureFlagClient;

  @BeforeEach
  void setup() {
    mGcsBucketSpecFetcher = mock(GcsBucketSpecFetcher.class);
    mConfigRepository = mock(ConfigRepository.class);
    mFeatureFlagClient = mock(TestClient.class);
    overrideProvider = new DefaultFeatureFlagDefinitionVersionOverrideProvider(mConfigRepository, mGcsBucketSpecFetcher, mFeatureFlagClient);
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE), any())).thenReturn("");
  }

  @Test
  void testGetVersionNoOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());
    verifyNoInteractions(mGcsBucketSpecFetcher);
    verifyNoInteractions(mConfigRepository);
  }

  @Test
  void testGetVersionWithOverride() throws IOException {
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE), any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2)).thenReturn(Optional.of(OVERRIDE_VERSION));

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);

    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verifyNoInteractions(mGcsBucketSpecFetcher);
    verify(mConfigRepository).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2);
    verifyNoMoreInteractions(mConfigRepository);
  }

  @Test
  void testGetVersionWithOverrideNotInDb() throws IOException {
    final ActorDefinitionVersion persistedADV = new ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withSpec(SPEC)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_IMAGE_TAG)
        .withActorDefinitionId(ACTOR_DEFINITION_ID);

    when(mGcsBucketSpecFetcher.attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2))).thenReturn(Optional.of(SPEC_2));
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE), any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2)).thenReturn(Optional.empty());
    when(mConfigRepository.writeActorDefinitionVersion(OVERRIDE_VERSION)).thenReturn(persistedADV);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);

    assertTrue(optResult.isPresent());
    assertEquals(persistedADV, optResult.get());
    verify(mConfigRepository).writeActorDefinitionVersion(OVERRIDE_VERSION);
    verify(mGcsBucketSpecFetcher).attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2));
  }

  @Test
  void testGetVersionWithFailedSpecFetch() throws IOException {
    when(mGcsBucketSpecFetcher.attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2)))
        .thenReturn(Optional.empty());

    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE), any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2)).thenReturn(Optional.empty());

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);

    assertTrue(optResult.isEmpty());
    verify(mGcsBucketSpecFetcher).attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2));
    verify(mConfigRepository).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG_2);
    verifyNoMoreInteractions(mConfigRepository);
  }

  @Test
  void testGetSourceContexts() {
    final List<Context> contexts = overrideProvider.getContexts(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID);

    final List<Context> expectedContexts = List.of(
        new Workspace(WORKSPACE_ID),
        new SourceDefinition(ACTOR_DEFINITION_ID),
        new Source(ACTOR_ID));

    assertEquals(expectedContexts, contexts);
  }

  @Test
  void testGetDestinationContexts() {
    final List<Context> contexts = overrideProvider.getContexts(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID);

    final List<Context> expectedContexts = List.of(
        new Workspace(WORKSPACE_ID),
        new DestinationDefinition(ACTOR_DEFINITION_ID),
        new Destination(ACTOR_ID));

    assertEquals(expectedContexts, contexts);
  }

  @Test
  void testGetSourceContextsNoActor() {
    final List<Context> contexts = overrideProvider.getContexts(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, null);

    final List<Context> expectedContexts = List.of(
        new Workspace(WORKSPACE_ID),
        new SourceDefinition(ACTOR_DEFINITION_ID));

    assertEquals(expectedContexts, contexts);
  }

  @Test
  void testGetDestinationContextsNoActor() {
    final List<Context> contexts = overrideProvider.getContexts(ActorType.DESTINATION, ACTOR_DEFINITION_ID, WORKSPACE_ID, null);

    final List<Context> expectedContexts = List.of(
        new Workspace(WORKSPACE_ID),
        new DestinationDefinition(ACTOR_DEFINITION_ID));

    assertEquals(expectedContexts, contexts);
  }

}
