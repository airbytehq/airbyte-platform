/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.version_overrides;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.AirbyteProtocolVersionRange;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ActorType;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.persistence.ActorDefinitionVersionResolver;
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

class DefaultDefinitionVersionOverrideProviderTest {

  private static final UUID ACTOR_DEFINITION_ID = UUID.randomUUID();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final UUID ACTOR_ID = UUID.randomUUID();
  private static final String DOCKER_REPOSITORY = "airbyte/source-test";
  private static final String DOCKER_IMAGE_TAG = "0.1.0";
  private static final String DOCKER_IMAGE_TAG_2 = "2.0.2";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification()
      .withProtocolVersion("0.2.0")
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "key", "value")));
  private static final ConnectorSpecification SPEC_2 = new ConnectorSpecification()
      .withProtocolVersion("0.2.0")
      .withConnectionSpecification(Jsons.jsonNode(Map.of(
          "theSpec", "goesHere")));
  private static final String DOCS_URL = "https://airbyte.io/docs/";
  private static final NormalizationDestinationDefinitionConfig NORMALIZATION_CONFIG = new NormalizationDestinationDefinitionConfig()
      .withNormalizationRepository("airbyte/normalization")
      .withNormalizationTag("tag")
      .withNormalizationIntegrationType("bigquery");
  private static final AllowedHosts ALLOWED_HOSTS = new AllowedHosts().withHosts(List.of("https://airbyte.io"));
  private static final SuggestedStreams SUGGESTED_STREAMS = new SuggestedStreams().withStreams(List.of("users"));
  private static final ActorDefinitionVersion DEFAULT_VERSION = new ActorDefinitionVersion()
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
  private static final ActorDefinitionVersion OVERRIDE_VERSION = new ActorDefinitionVersion()
      .withDockerRepository(DOCKER_REPOSITORY)
      .withActorDefinitionId(ACTOR_DEFINITION_ID)
      .withDockerImageTag(DOCKER_IMAGE_TAG_2)
      .withSpec(SPEC_2)
      .withProtocolVersion(SPEC_2.getProtocolVersion())
      .withDocumentationUrl(DOCS_URL)
      .withReleaseStage(ReleaseStage.BETA)
      .withSuggestedStreams(SUGGESTED_STREAMS)
      .withAllowedHosts(ALLOWED_HOSTS)
      .withSupportsDbt(true)
      .withNormalizationConfig(NORMALIZATION_CONFIG);
  private static final AirbyteProtocolVersionRange PROTOCOL_VERSION_RANGE =
      new AirbyteProtocolVersionRange(new Version("0.0.0"), new Version("0.3.0"));

  private DefaultDefinitionVersionOverrideProvider overrideProvider;
  private ActorDefinitionVersionResolver mActorDefinitionVersionResolver;
  private FeatureFlagClient mFeatureFlagClient;

  @BeforeEach
  void setup() {
    mActorDefinitionVersionResolver = mock(ActorDefinitionVersionResolver.class);
    mFeatureFlagClient = mock(TestClient.class);
    overrideProvider =
        new DefaultDefinitionVersionOverrideProvider(mActorDefinitionVersionResolver, mFeatureFlagClient, PROTOCOL_VERSION_RANGE);
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE), any())).thenReturn("");
  }

  @Test
  void testGetVersionNoOverride() {
    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION);
    assertTrue(optResult.isEmpty());

    verifyNoInteractions(mActorDefinitionVersionResolver);
  }

  @Test
  void testGetVersionWithOverride() throws IOException {
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE),
        any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mActorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2))
        .thenReturn(Optional.of(OVERRIDE_VERSION));

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID,
            DEFAULT_VERSION);

    assertEquals(OVERRIDE_VERSION, optResult.orElse(null));
    verify(mActorDefinitionVersionResolver).resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2);
    verifyNoMoreInteractions(mActorDefinitionVersionResolver);
  }

  @Test
  void testOverrideIsEmptyIfVersionDoesNotResolve() throws IOException {
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE),
        any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mActorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2))
        .thenReturn(Optional.empty());

    final Optional<ActorDefinitionVersion> optResult =
        overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID,
            DEFAULT_VERSION);

    assertEquals(Optional.empty(), optResult);
    verify(mActorDefinitionVersionResolver).resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2);
    verifyNoMoreInteractions(mActorDefinitionVersionResolver);
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

  @Test
  void testGetVersionWithInvalidProtocolVersion() throws IOException {
    when(mFeatureFlagClient.stringVariation(eq(ConnectorVersionOverride.INSTANCE),
        any())).thenReturn(DOCKER_IMAGE_TAG_2);
    when(mActorDefinitionVersionResolver.resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2))
        .thenReturn(
            Optional.of(Jsons.clone(OVERRIDE_VERSION).withProtocolVersion("131.1.2")));

    assertThrows(RuntimeException.class,
        () -> overrideProvider.getOverride(ActorType.SOURCE, ACTOR_DEFINITION_ID, WORKSPACE_ID, ACTOR_ID, DEFAULT_VERSION));
    verify(mActorDefinitionVersionResolver).resolveVersionForTag(ACTOR_DEFINITION_ID, ActorType.SOURCE, DOCKER_REPOSITORY, DOCKER_IMAGE_TAG_2);
    verifyNoMoreInteractions(mActorDefinitionVersionResolver);
  }

}
