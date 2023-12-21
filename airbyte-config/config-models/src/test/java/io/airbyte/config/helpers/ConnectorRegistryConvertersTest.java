/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionResourceRequirements;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleases;
import io.airbyte.config.NormalizationDestinationDefinitionConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.protocol.models.ConnectorSpecification;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  private static final BreakingChangeScope breakingChangeScope = new BreakingChangeScope()
      .withScopeType(ScopeType.STREAM)
      .withImpactedScopes(List.of("stream1", "stream2"));

  private static final BreakingChanges registryBreakingChanges = new BreakingChanges().withAdditionalProperty("1.0.0", new VersionBreakingChange()
      .withMessage("Sample message").withUpgradeDeadline("2023-07-20").withMigrationDocumentationUrl("https://example.com").withScopedImpact(
          List.of(breakingChangeScope)));

  private static final BreakingChanges registryBreakingChangesWithoutScopedImpact =
      new BreakingChanges().withAdditionalProperty("1.0.0", new VersionBreakingChange()
          .withMessage("Sample message").withUpgradeDeadline("2023-07-20").withMigrationDocumentationUrl("https://example.com"));
  private static final List<ActorDefinitionBreakingChange> expectedBreakingChanges = List.of(new ActorDefinitionBreakingChange()
      .withActorDefinitionId(DEF_ID)
      .withVersion(new Version("1.0.0"))
      .withMigrationDocumentationUrl("https://example.com")
      .withUpgradeDeadline("2023-07-20")
      .withMessage("Sample message")
      .withScopedImpact(List.of(breakingChangeScope)));

  @Test
  void testConvertRegistrySourceToInternalTypes() {
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
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion("doesnt matter")
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withSuggestedStreams(suggestedStreams)
        .withMaxSecondsBetweenMessages(10L)
        .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

    final StandardSourceDefinition stdSourceDef = new StandardSourceDefinition()
        .withSourceDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withMaxSecondsBetweenMessages(10L);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withSuggestedStreams(suggestedStreams);

    assertEquals(stdSourceDef, ConnectorRegistryConverters.toStandardSourceDefinition(registrySourceDef));
    assertEquals(actorDefinitionVersion, ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDef));
    assertEquals(expectedBreakingChanges, ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef));
  }

  @Test
  void testConvertRegistrySourceDefaults() {
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
        .withProtocolVersion("doesnt matter")
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withSuggestedStreams(suggestedStreams)
        .withMaxSecondsBetweenMessages(10L)
        .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

    final ActorDefinitionVersion convertedAdv = ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDef);
    assertEquals(SupportLevel.NONE, convertedAdv.getSupportLevel());
  }

  @Test
  void testConvertRegistryDestinationToInternalTypes() {
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
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withNormalizationConfig(normalizationConfig)
        .withSupportsDbt(true)
        .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

    final StandardDestinationDefinition stdDestinationDef = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withResourceRequirements(RESOURCE_REQUIREMENTS);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withNormalizationConfig(normalizationConfig)
        .withSupportsDbt(true);

    assertEquals(stdDestinationDef, ConnectorRegistryConverters.toStandardDestinationDefinition(registryDestinationDef));
    assertEquals(actorDefinitionVersion, ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDef));
    assertEquals(expectedBreakingChanges, ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef));
  }

  @Test
  void testConvertRegistryDestinationDefaults() {
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
        .withSupportsDbt(true)
        .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChanges));

    final ActorDefinitionVersion convertedAdv = ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDef);
    assertEquals(SupportLevel.NONE, convertedAdv.getSupportLevel());
  }

  @Test
  void testConvertRegistryDestinationWithoutScopedImpact() {
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
        .withSupportsDbt(true)
        .withReleases(new ConnectorReleases().withBreakingChanges(registryBreakingChangesWithoutScopedImpact));

    final List<ActorDefinitionBreakingChange> actorDefinitionBreakingChanges =
        ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef);
    assertEquals(expectedBreakingChanges.size(), actorDefinitionBreakingChanges.size());
    for (final ActorDefinitionBreakingChange actorDefinitionBreakingChange : actorDefinitionBreakingChanges) {
      assertEquals(actorDefinitionBreakingChange.getScopedImpact(), Collections.emptyList());
    }
  }

  @Test
  void testParseSourceDefinitionWithNoBreakingChangesReturnsEmptyList() {
    ConnectorRegistrySourceDefinition registrySourceDef = new ConnectorRegistrySourceDefinition();
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef), Collections.emptyList());

    registrySourceDef = new ConnectorRegistrySourceDefinition().withReleases(new ConnectorReleases());
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef), Collections.emptyList());

    registrySourceDef = new ConnectorRegistrySourceDefinition().withReleases(new ConnectorReleases().withBreakingChanges(new BreakingChanges()));
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef), Collections.emptyList());
  }

  @Test
  void testParseDestinationDefinitionWithNoBreakingChangesReturnsEmptyList() {
    ConnectorRegistryDestinationDefinition registryDestinationDef = new ConnectorRegistryDestinationDefinition();
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());

    registryDestinationDef = new ConnectorRegistryDestinationDefinition().withReleases(new ConnectorReleases());
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());

    registryDestinationDef =
        new ConnectorRegistryDestinationDefinition().withReleases(new ConnectorReleases().withBreakingChanges(new BreakingChanges()));
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());
  }

  @ParameterizedTest
  @CsvSource({"0.0.1, true", "dev, true", "test, false", "1.9.1-dev.33a53e6236, true", "97b69a76-1f06-4680-8905-8beda74311d0, false"})
  void testDockerImageValidation(final String dockerImageTag, final boolean isValid) {
    final ConnectorRegistrySourceDefinition registrySourceDefinition = new ConnectorRegistrySourceDefinition()
        .withDockerImageTag(dockerImageTag);
    final ConnectorRegistryDestinationDefinition registryDestinationDefinition = new ConnectorRegistryDestinationDefinition()
        .withDockerImageTag(dockerImageTag);

    if (isValid) {
      assertDoesNotThrow(() -> ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition));
      assertDoesNotThrow(() -> ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDefinition));
    } else {
      assertThrows(IllegalArgumentException.class, () -> ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDefinition));
      assertThrows(IllegalArgumentException.class, () -> ConnectorRegistryConverters.toActorDefinitionVersion(registryDestinationDefinition));
    }
  }

}
