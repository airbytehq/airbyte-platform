/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ConnectorRegistryConvertersTest {

  private static final UUID DEF_ID = UUID.randomUUID();
  private static final String CONNECTOR_NAME = "postgres";
  private static final String DOCKER_REPOSITORY = "airbyte/postgres";
  private static final String DOCKER_TAG = "0.1.0";
  private static final String DOCS_URL = "https://airbyte.com";
  private static final String RELEASE_DATE = "2021-01-01";
  private static final String PROTOCOL_VERSION = "1.0.0";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification().withConnectionSpecification(
      Jsons.jsonNode(ImmutableMap.of("key", "val"))).withProtocolVersion(PROTOCOL_VERSION);
  private static final AllowedHosts ALLOWED_HOSTS = new AllowedHosts().withHosts(List.of("host1", "host2"));
  private static final ActorDefinitionResourceRequirements RESOURCE_REQUIREMENTS =
      new ActorDefinitionResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2"));

  @Test
  void testConvertRegistrySourceToStandardSourceDef() {
    final SuggestedStreams suggestedStreams = new SuggestedStreams().withStreams(List.of("stream1", "stream2"));

    final ConnectorRegistrySourceDefinition registrySourceDef = new ConnectorRegistrySourceDefinition()
        .withSourceDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withDocumentationUrl(DOCS_URL)
        .withSpec(SPEC)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withSuggestedStreams(suggestedStreams)
        .withMaxSecondsBetweenMessages(10L);

    final StandardSourceDefinition stdSourceDef = new StandardSourceDefinition()
        .withSourceDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withName(CONNECTOR_NAME)
        .withDocumentationUrl(DOCS_URL)
        .withSpec(SPEC)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withSuggestedStreams(suggestedStreams)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withMaxSecondsBetweenMessages(10L);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withSuggestedStreams(suggestedStreams);

    assertEquals(stdSourceDef, ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDef));
    assertEquals(actorDefinitionVersion, ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDef));
  }

  @Test
  void testConvertRegistryDestinationToStandardDestinationDef() {
    final NormalizationDestinationDefinitionConfig normalizationConfig = new NormalizationDestinationDefinitionConfig()
        .withNormalizationRepository("normalization")
        .withNormalizationTag("0.1.0")
        .withNormalizationIntegrationType("bigquery");

    final ConnectorRegistryDestinationDefinition registryDestinationDef = new ConnectorRegistryDestinationDefinition()
        .withDestinationDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withDocumentationUrl(DOCS_URL)
        .withSpec(SPEC)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withNormalizationConfig(normalizationConfig)
        .withSupportsDbt(true);

    final StandardDestinationDefinition stdDestinationDef = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withName(CONNECTOR_NAME)
        .withDocumentationUrl(DOCS_URL)
        .withSpec(SPEC)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withNormalizationConfig(normalizationConfig)
        .withSupportsDbt(true);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withNormalizationConfig(normalizationConfig)
        .withSupportsDbt(true);

    assertEquals(stdDestinationDef, ConnectorRegistryConverters.toStandardDestinationDefinition(registryDestinationDef));
    assertEquals(actorDefinitionVersion, ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDef));
  }

}
