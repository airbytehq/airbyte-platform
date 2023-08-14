/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.helpers.ConnectorRegistryConverters;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.specs.RemoteDefinitionsProvider;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActorDefinitionVersionResolverTest {

  private ActorDefinitionVersionResolver actorDefinitionVersionResolver;
  private RemoteDefinitionsProvider mRemoteDefinitionsProvider;
  private ConfigRepository mConfigRepository;

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withProtocolVersion("0.2.0")
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final String DOCS_URL = "https://airbyte.io/docs/";
  private static final NormalizationDestinationDefinitionConfig NORMALIZATION_CONFIG = new NormalizationDestinationDefinitionConfig()
      .withNormalizationRepository("airbyte/normalization")
      .withNormalizationTag("tag")
      .withNormalizationIntegrationType("bigquery");
  private static final AllowedHosts ALLOWED_HOSTS = new AllowedHosts().withHosts(List.of("https://airbyte.io"));
  private static final SuggestedStreams SUGGESTED_STREAMS = new SuggestedStreams().withStreams(List.of("users"));
  private static final ActorDefinitionVersion ACTOR_DEFINITION_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG)
      .withSpec(SPEC)
      .withProtocolVersion(SPEC.getProtocolVersion())
      .withDocumentationUrl(DOCS_URL)
      .withReleaseStage(ReleaseStage.BETA)
      .withSuggestedStreams(SUGGESTED_STREAMS)
      .withAllowedHosts(ALLOWED_HOSTS)
      .withSupportsDbt(true)
      .withNormalizationConfig(NORMALIZATION_CONFIG);

  private static final ConnectorRegistrySourceDefinition REGISTRY_DEF = new ConnectorRegistrySourceDefinition()
      .withSourceDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerRepository(ACTOR_DEFINITION_VERSION.getDockerRepository())
      .withDockerImageTag(ACTOR_DEFINITION_VERSION.getDockerImageTag())
      .withSpec(ACTOR_DEFINITION_VERSION.getSpec())
      .withProtocolVersion(ACTOR_DEFINITION_VERSION.getProtocolVersion())
      .withDocumentationUrl(ACTOR_DEFINITION_VERSION.getDocumentationUrl())
      .withReleaseStage(ACTOR_DEFINITION_VERSION.getReleaseStage())
      .withSuggestedStreams(ACTOR_DEFINITION_VERSION.getSuggestedStreams())
      .withAllowedHosts(ACTOR_DEFINITION_VERSION.getAllowedHosts());

  @BeforeEach
  void setup() {
    mRemoteDefinitionsProvider = mock(RemoteDefinitionsProvider.class);
    mConfigRepository = mock(ConfigRepository.class);
    actorDefinitionVersionResolver = new ActorDefinitionVersionResolver(mRemoteDefinitionsProvider, mConfigRepository);
  }

  @Test
  void testResolveVersionFromDB() throws IOException {
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)).thenReturn(Optional.of(ACTOR_DEFINITION_VERSION));

    assertEquals(Optional.of(ACTOR_DEFINITION_VERSION),
        actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG));

    verify(mConfigRepository).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    verifyNoMoreInteractions(mConfigRepository);

    verifyNoInteractions(mRemoteDefinitionsProvider);
  }

  @Test
  void testResolveVersionFromRemoteIfNotInDB() throws IOException {

    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)).thenReturn(Optional.empty());
    when(mRemoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)).thenReturn(Optional.of(REGISTRY_DEF));

    final ActorDefinitionVersion actorDefinitionVersion =
        ConnectorRegistryConverters.toActorDefinitionVersion(REGISTRY_DEF);
    final ActorDefinitionVersion persistedAdv =
        Jsons.clone(actorDefinitionVersion).withVersionId(UUID.randomUUID());
    when(mConfigRepository.writeActorDefinitionVersion(actorDefinitionVersion)).thenReturn(persistedAdv);

    final Optional<ActorDefinitionVersion> optResult =
        actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG);
    assertTrue(optResult.isPresent());
    assertEquals(persistedAdv, optResult.get());

    verify(mConfigRepository).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    verify(mConfigRepository).writeActorDefinitionVersion(actorDefinitionVersion);
    verifyNoMoreInteractions(mConfigRepository);

    verify(mRemoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG);
    verifyNoMoreInteractions(mRemoteDefinitionsProvider);
  }

  @Test
  void testReturnsEmptyOptionalIfNoVersionFoundInDbOrRemote() throws IOException {
    when(mConfigRepository.getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG)).thenReturn(Optional.empty());
    when(mRemoteDefinitionsProvider.getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG)).thenReturn(Optional.empty());

    assertTrue(
        actorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG).isEmpty());

    verify(mConfigRepository).getActorDefinitionVersion(ACTOR_DEFINITION_ID, DOCKER_IMAGE_TAG);
    verifyNoMoreInteractions(mConfigRepository);

    verify(mRemoteDefinitionsProvider).getSourceDefinitionByVersion(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG);
    verifyNoMoreInteractions(mRemoteDefinitionsProvider);
  }

}
