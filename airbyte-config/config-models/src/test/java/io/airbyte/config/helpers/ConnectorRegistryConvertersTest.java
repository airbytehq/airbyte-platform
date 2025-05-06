/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.version.Version;
import io.airbyte.config.AbInternal;
import io.airbyte.config.ActorDefinitionBreakingChange;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AllowedHosts;
import io.airbyte.config.BreakingChangeScope;
import io.airbyte.config.BreakingChangeScope.ScopeType;
import io.airbyte.config.BreakingChanges;
import io.airbyte.config.ConnectorEnumRolloutState;
import io.airbyte.config.ConnectorRegistryDestinationDefinition;
import io.airbyte.config.ConnectorRegistrySourceDefinition;
import io.airbyte.config.ConnectorReleasesDestination;
import io.airbyte.config.ConnectorReleasesSource;
import io.airbyte.config.ConnectorRollout;
import io.airbyte.config.ReleaseCandidatesDestination;
import io.airbyte.config.ReleaseCandidatesSource;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.RolloutConfiguration;
import io.airbyte.config.ScopedResourceRequirements;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.SuggestedStreams;
import io.airbyte.config.SupportLevel;
import io.airbyte.config.VersionBreakingChange;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
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
  private static final String LANGUAGE = "manifest-only";

  private static final String SAMPLE_MESSAGE = "Sample message";

  private static final String UPGRADE_DEADLINE = "2023-07-20";

  private static final String DEADLINE_ACTION = "upgrade";

  private static final String DOCUMENTATION_URL = "https://example.com";
  private static final ConnectorSpecification SPEC = new ConnectorSpecification().withConnectionSpecification(
      Jsons.jsonNode(ImmutableMap.of("key", "val"))).withProtocolVersion(PROTOCOL_VERSION);
  private static final AllowedHosts ALLOWED_HOSTS = new AllowedHosts().withHosts(List.of("host1", "host2"));
  private static final ScopedResourceRequirements RESOURCE_REQUIREMENTS =
      new ScopedResourceRequirements().withDefault(new ResourceRequirements().withCpuRequest("2"));

  private static final BreakingChangeScope breakingChangeScope = new BreakingChangeScope()
      .withScopeType(ScopeType.STREAM)
      .withImpactedScopes(List.of("stream1", "stream2"));

  private static final BreakingChanges sourceRegistryBreakingChanges = new BreakingChanges().withAdditionalProperty(PROTOCOL_VERSION,
      new VersionBreakingChange()
          .withMessage(SAMPLE_MESSAGE).withUpgradeDeadline(UPGRADE_DEADLINE).withDeadlineAction(DEADLINE_ACTION)
          .withMigrationDocumentationUrl(DOCUMENTATION_URL).withScopedImpact(
              List.of(breakingChangeScope)));

  private static final BreakingChanges destinationBreakingChanges =
      new BreakingChanges().withAdditionalProperty(PROTOCOL_VERSION,
          new VersionBreakingChange()
              .withMessage(SAMPLE_MESSAGE).withUpgradeDeadline(UPGRADE_DEADLINE).withDeadlineAction(DEADLINE_ACTION)
              .withMigrationDocumentationUrl(DOCUMENTATION_URL).withScopedImpact(
                  List.of(breakingChangeScope)));

  private static final BreakingChanges destinationRegistryBreakingChangesWithoutScopedImpact =
      new BreakingChanges().withAdditionalProperty(PROTOCOL_VERSION, new VersionBreakingChange()
          .withMessage(SAMPLE_MESSAGE).withUpgradeDeadline(UPGRADE_DEADLINE).withMigrationDocumentationUrl(DOCUMENTATION_URL));

  private static final List<ActorDefinitionBreakingChange> expectedBreakingChanges = List.of(new ActorDefinitionBreakingChange()
      .withActorDefinitionId(DEF_ID)
      .withVersion(new Version(PROTOCOL_VERSION))
      .withMigrationDocumentationUrl(DOCUMENTATION_URL)
      .withUpgradeDeadline(UPGRADE_DEADLINE)
      .withDeadlineAction(DEADLINE_ACTION)
      .withMessage(SAMPLE_MESSAGE)
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
        .withAbInternal(new AbInternal().withSl(200L).withIsEnterprise(true))
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion("doesnt matter")
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withSuggestedStreams(suggestedStreams)
        .withMaxSecondsBetweenMessages(10L)
        .withSupportsFileTransfer(true)
        .withReleases(new ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges));

    final StandardSourceDefinition stdSourceDef = new StandardSourceDefinition()
        .withSourceDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withEnterprise(true)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withMaxSecondsBetweenMessages(10L);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withSuggestedStreams(suggestedStreams)
        .withSupportsFileTransfer(true);

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
        .withReleases(new ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges));

    final ActorDefinitionVersion convertedAdv = ConnectorRegistryConverters.toActorDefinitionVersion(registrySourceDef);
    assertEquals(SupportLevel.NONE, convertedAdv.getSupportLevel());
    assertFalse(convertedAdv.getSupportsFileTransfer());
  }

  @Test
  void testConvertRegistryDestinationToInternalTypes() {
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
        .withAbInternal(new AbInternal().withSl(200L).withIsEnterprise(true))
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withReleases(new ConnectorReleasesDestination().withBreakingChanges(destinationBreakingChanges))
        .withLanguage(LANGUAGE)
        .withSupportsFileTransfer(true)
        .withSupportsDataActivation(true);

    final StandardDestinationDefinition stdDestinationDef = new StandardDestinationDefinition()
        .withDestinationDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withEnterprise(true)
        .withCustom(false)
        .withResourceRequirements(RESOURCE_REQUIREMENTS);

    final ActorDefinitionVersion actorDefinitionVersion = new ActorDefinitionVersion()
        .withActorDefinitionId(DEF_ID)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withDockerImageTag(DOCKER_TAG)
        .withSpec(SPEC)
        .withDocumentationUrl(DOCS_URL)
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withLanguage(LANGUAGE)
        .withSupportsFileTransfer(true)
        .withSupportsDataActivation(true);

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
        .withReleases(new ConnectorReleasesDestination().withBreakingChanges(destinationBreakingChanges));

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
        .withReleases(new ConnectorReleasesDestination().withBreakingChanges(destinationRegistryBreakingChangesWithoutScopedImpact));

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

    registrySourceDef = new ConnectorRegistrySourceDefinition().withReleases(new ConnectorReleasesSource());
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef), Collections.emptyList());

    registrySourceDef =
        new ConnectorRegistrySourceDefinition().withReleases(new ConnectorReleasesSource().withBreakingChanges(new BreakingChanges()));
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registrySourceDef), Collections.emptyList());
  }

  @Test
  void testParseDestinationDefinitionWithNoBreakingChangesReturnsEmptyList() {
    ConnectorRegistryDestinationDefinition registryDestinationDef = new ConnectorRegistryDestinationDefinition();
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());

    registryDestinationDef = new ConnectorRegistryDestinationDefinition().withReleases(new ConnectorReleasesDestination());
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());

    registryDestinationDef =
        new ConnectorRegistryDestinationDefinition()
            .withReleases(new ConnectorReleasesDestination().withBreakingChanges(new BreakingChanges()));
    assertEquals(ConnectorRegistryConverters.toActorDefinitionBreakingChanges(registryDestinationDef), Collections.emptyList());
  }

  @Test
  void testToReleaseCandidateSourceDefinitions() {
    ConnectorRegistrySourceDefinition registrySourceDef = new ConnectorRegistrySourceDefinition();
    assertEquals(ConnectorRegistryConverters.toRcSourceDefinitions(registrySourceDef), Collections.emptyList());

    registrySourceDef = new ConnectorRegistrySourceDefinition()
        .withReleases(new ConnectorReleasesSource().withReleaseCandidates(new ReleaseCandidatesSource().withAdditionalProperty(
            DOCKER_TAG,
            new ConnectorRegistrySourceDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY))));
    List<ConnectorRegistrySourceDefinition> rcDefs = ConnectorRegistryConverters.toRcSourceDefinitions(registrySourceDef);
    assertEquals(rcDefs.size(), 1);
    assertEquals(rcDefs.get(0).getDockerImageTag(), DOCKER_TAG);
    assertEquals(rcDefs.get(0).getDockerRepository(), DOCKER_REPOSITORY);

    registrySourceDef = new ConnectorRegistrySourceDefinition()
        .withReleases(new ConnectorReleasesSource().withReleaseCandidates(new ReleaseCandidatesSource().withAdditionalProperty(
            DOCKER_TAG,
            null)));
    rcDefs = ConnectorRegistryConverters.toRcSourceDefinitions(registrySourceDef);
    assertEquals(rcDefs.size(), 0);

  }

  @Test
  void testToReleaseCandidateDestinationDefinitions() {
    ConnectorRegistryDestinationDefinition registryDestinationDef = new ConnectorRegistryDestinationDefinition();
    assertEquals(ConnectorRegistryConverters.toRcDestinationDefinitions(registryDestinationDef), Collections.emptyList());

    registryDestinationDef = new ConnectorRegistryDestinationDefinition()
        .withReleases(new ConnectorReleasesDestination().withReleaseCandidates(new ReleaseCandidatesDestination().withAdditionalProperty(
            DOCKER_TAG,
            new ConnectorRegistryDestinationDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY))));

    List<ConnectorRegistryDestinationDefinition> rcDefs =
        ConnectorRegistryConverters.toRcDestinationDefinitions(registryDestinationDef);
    assertEquals(rcDefs.size(), 1);
    assertEquals(rcDefs.get(0).getDockerImageTag(), DOCKER_TAG);
    assertEquals(rcDefs.get(0).getDockerRepository(), DOCKER_REPOSITORY);

    registryDestinationDef = new ConnectorRegistryDestinationDefinition()
        .withReleases(new ConnectorReleasesDestination().withReleaseCandidates(new ReleaseCandidatesDestination().withAdditionalProperty(
            DOCKER_TAG,
            null)));
    rcDefs = ConnectorRegistryConverters.toRcDestinationDefinitions(registryDestinationDef);
    assertEquals(rcDefs.size(), 0);

  }

  @Test
  void testToConnectorRollout() {
    UUID advId = UUID.randomUUID();
    UUID initialAdvId = UUID.randomUUID();
    UUID actorDefinitionId = UUID.randomUUID();
    RolloutConfiguration rolloutConfiguration =
        new RolloutConfiguration().withAdvanceDelayMinutes(1L).withInitialPercentage(10L).withMaxPercentage(100L);
    ConnectorRegistrySourceDefinition rcDef =
        new ConnectorRegistrySourceDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY)
            .withSourceDefinitionId(actorDefinitionId).withReleases(new ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration));
    ActorDefinitionVersion rcAdv = new ActorDefinitionVersion().withActorDefinitionId(actorDefinitionId).withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY);
    ActorDefinitionVersion initialAdv = new ActorDefinitionVersion().withActorDefinitionId(actorDefinitionId).withVersionId(initialAdvId)
        .withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY);

    // Normal behavior

    ConnectorRollout rollout = ConnectorRegistryConverters.toConnectorRollout(rcDef, rcAdv, initialAdv);

    assertEquals(actorDefinitionId, rollout.getActorDefinitionId());
    assertEquals(rolloutConfiguration.getInitialPercentage().intValue(), rollout.getInitialRolloutPct());
    assertEquals(rolloutConfiguration.getMaxPercentage().intValue(), rollout.getFinalTargetRolloutPct());
    assertEquals(rolloutConfiguration.getAdvanceDelayMinutes().intValue(), rollout.getMaxStepWaitTimeMins());
    assertEquals(ConnectorEnumRolloutState.INITIALIZED, rollout.getState());

    // With dockerImageTag mismatch
    ConnectorRegistrySourceDefinition rcDefWithDockerImageTagMismatch =
        new ConnectorRegistrySourceDefinition().withDockerImageTag("1.0.0").withDockerRepository(DOCKER_REPOSITORY)
            .withSourceDefinitionId(actorDefinitionId).withReleases(new ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration));
    ActorDefinitionVersion rcAdvWithDockerImageTagMismatch = new ActorDefinitionVersion().withActorDefinitionId(actorDefinitionId)
        .withVersionId(advId).withDockerImageTag("1.1.0").withDockerRepository(DOCKER_REPOSITORY);
    assertThrows(AssertionError.class, () -> {
      ConnectorRegistryConverters.toConnectorRollout(rcDefWithDockerImageTagMismatch, rcAdvWithDockerImageTagMismatch, initialAdv);
    });

    // With definition id mismatch
    ConnectorRegistrySourceDefinition rcDefWithDefinitionIdTagMismatch =
        new ConnectorRegistrySourceDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY)
            .withSourceDefinitionId(actorDefinitionId).withReleases(new ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration));
    ActorDefinitionVersion rcAdvWithDefinitionIdMismatch = new ActorDefinitionVersion().withActorDefinitionId(advId).withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY);
    assertThrows(AssertionError.class, () -> {
      ConnectorRegistryConverters.toConnectorRollout(rcDefWithDefinitionIdTagMismatch, rcAdvWithDefinitionIdMismatch, initialAdv);
    });

    // With docker repository mismatch
    ConnectorRegistrySourceDefinition rcDefDockerRepoMismatch =
        new ConnectorRegistrySourceDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository("airbyte/source-faker")
            .withSourceDefinitionId(actorDefinitionId).withReleases(new ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration));
    ActorDefinitionVersion rcAdvDockerRepoMismatch = new ActorDefinitionVersion().withActorDefinitionId(advId).withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG).withDockerRepository("airbyte/source-mismatch");
    assertThrows(AssertionError.class, () -> {
      ConnectorRegistryConverters.toConnectorRollout(rcDefDockerRepoMismatch, rcAdvDockerRepoMismatch, initialAdv);
    });
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
