/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorDefinitionVersionOverride;
import io.airbyte.config.ActorType;
import io.airbyte.config.VersionOverride;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultDefinitionVersionOverrideProviderTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.fromString("dfd88b22-b603-4c3d-aad7-3701784586b1");
  private static final UUID OVERRIDDEN_ACTOR_ID = UUID.fromString("f40167fa-0828-416f-a9cf-7408ed4ac0ba");
  private static final UUID OVERRIDDEN_WORKSPACE_ID = UUID.fromString("5abfdb68-211c-4442-8448-785b0e3efe13");
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "2.0.2";
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

  private DefaultDefinitionVersionOverrideProvider overrideProvider;

  @BeforeEach
  void setup() {
    overrideProvider = new DefaultDefinitionVersionOverrideProvider(DefaultDefinitionVersionOverrideProviderTest.class);
  }

  @Test
  void testGetVersionNoOverride() {
    final UUID newActorId = UUID.randomUUID();
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ACTOR_DEFINITION_ID, newActorId, OverrideTargetType.ACTOR, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());
  }

  @Test
  void testGetVersionWithActorOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ACTOR_DEFINITION_ID, OVERRIDDEN_ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION);
    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
  }

  @Test
  void testGetVersionWithWorkspaceOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ACTOR_DEFINITION_ID, OVERRIDDEN_WORKSPACE_ID, OverrideTargetType.WORKSPACE, DEFAULT_VERSION);
    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
  }

  @Test
  void testGetVersionWithInvalidOverride() {
    final Map<UUID, ActorDefinitionVersionOverride> overridesMap = Map.of(
        ACTOR_DEFINITION_ID, new ActorDefinitionVersionOverride()
            .withActorDefinitionId(ACTOR_DEFINITION_ID)
            .withActorType(ActorType.SOURCE)
            .withVersionOverrides(List.of(
                new VersionOverride()
                    .withActorDefinitionVersion(new ActorDefinitionVersion()
                        .withDockerImageTag(DOCKER_IMAGE_TAG_2)) // missing spec declaration
                    .withActorIds(List.of(OVERRIDDEN_ACTOR_ID)))));

    overrideProvider = new DefaultDefinitionVersionOverrideProvider(overridesMap);

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ACTOR_DEFINITION_ID, OVERRIDDEN_ACTOR_ID, OverrideTargetType.ACTOR, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());
  }

}
