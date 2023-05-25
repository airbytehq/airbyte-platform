/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersionOverride;
import io.airbyte.config.ActorType;
import io.airbyte.config.VersionOverride;
import io.airbyte.config.specs.GcsBucketSpecFetcher;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultLocalDefinitionVersionOverrideProviderTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.fromString("dfd88b22-b603-4c3d-aad7-3701784586b1");
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final UUID OVERRIDDEN_ACTOR_ID = UUID.fromString("f40167fa-0828-416f-a9cf-7408ed4ac0ba");
  private static final UUID OVERRIDDEN_WORKSPACE_ID = UUID.fromString("5abfdb68-211c-4442-8448-785b0e3efe13");
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
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC);
  private static final ActorDefinitionVersion OVERRIDE_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2);

  private DefaultLocalDefinitionVersionOverrideProvider overrideProvider;
  private GcsBucketSpecFetcher mGcsBucketSpecFetcher;

  @BeforeEach
  void setup() {
    mGcsBucketSpecFetcher = mock(GcsBucketSpecFetcher.class);
    overrideProvider = new DefaultLocalDefinitionVersionOverrideProvider(
        DefaultLocalDefinitionVersionOverrideProviderTest.class, "version_overrides_test.yml",
        mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionNoOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());
    verifyNoInteractions(mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionWithActorOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, OVERRIDDEN_ACTOR_ID, DEFAULT_VERSION);
    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verifyNoInteractions(mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionWithWorkspaceOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, OVERRIDDEN_WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);
    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verifyNoInteractions(mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionWithWorkspaceOverrideNoActor() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, OVERRIDDEN_WORKSPACE_ID, null, DEFAULT_VERSION);
    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verifyNoInteractions(mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionWithInvalidOverride() {
    final Map<UUID, ActorDefinitionVersionOverride> overridesMap = Map.of(
        ACTOR_DEFINITION_ID, new ActorDefinitionVersionOverride()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withActorType(ActorType.SOURCE)
            .withVersionOverrides(List.of(
                new VersionOverride()
                    .withActorDefinitionVersion(new ActorDefinitionVersion()) // missing image tag
                    .withActorIds(List.of(OVERRIDDEN_ACTOR_ID)))));

    overrideProvider = new DefaultLocalDefinitionVersionOverrideProvider(overridesMap, mGcsBucketSpecFetcher);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverrideForTarget(ACTOR_DEFINITION_ID, OVERRIDDEN_ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());
    verifyNoInteractions(mGcsBucketSpecFetcher);
  }

  @Test
  void testGetVersionWithMissingSpec() {
    when(mGcsBucketSpecFetcher.attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2))).thenReturn(Optional.of(SPEC_2));

    final Map<UUID, ActorDefinitionVersionOverride> overridesMap = Map.of(
        ACTOR_DEFINITION_ID, new ActorDefinitionVersionOverride()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withActorType(ActorType.SOURCE)
            .withVersionOverrides(List.of(
                new VersionOverride()
                    .withActorDefinitionVersion(new ActorDefinitionVersion()
                        .withDockerImageTag(DOCKER_IMAGE_TAG_2)) // missing spec declaration
                    .withActorIds(List.of(OVERRIDDEN_ACTOR_ID)))));

    overrideProvider = new DefaultLocalDefinitionVersionOverrideProvider(overridesMap, mGcsBucketSpecFetcher);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverrideForTarget(ACTOR_DEFINITION_ID, OVERRIDDEN_ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION);

    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verify(mGcsBucketSpecFetcher).attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2));
  }

  @Test
  void testGetVersionWithMissingSpecAndMissingFetch() {
    when(mGcsBucketSpecFetcher.attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2)))
        .thenReturn(Optional.empty());

    final Map<UUID, ActorDefinitionVersionOverride> overridesMap = Map.of(
        ACTOR_DEFINITION_ID, new ActorDefinitionVersionOverride()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withActorType(ActorType.SOURCE)
            .withVersionOverrides(List.of(
                new VersionOverride()
                    .withActorDefinitionVersion(new ActorDefinitionVersion()
                        .withDockerImageTag(DOCKER_IMAGE_TAG_2)) // missing spec declaration
                    .withActorIds(List.of(OVERRIDDEN_ACTOR_ID)))));

    overrideProvider = new DefaultLocalDefinitionVersionOverrideProvider(overridesMap, mGcsBucketSpecFetcher);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverrideForTarget(ACTOR_DEFINITION_ID, OVERRIDDEN_ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION);

    assertTrue(optResult.isEmpty());
    verify(mGcsBucketSpecFetcher).attemptFetch(String.format(DOCKER_IMG_FORMAT, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2));
  }

}
