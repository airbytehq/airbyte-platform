/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.google.common.collect.ImmutableMap
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.version.Version
import io.airbyte.config.AbInternal
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.AllowedHosts
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.BreakingChanges
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ConnectorReleasesDestination
import io.airbyte.config.ConnectorReleasesSource
import io.airbyte.config.ReleaseCandidatesDestination
import io.airbyte.config.ReleaseCandidatesSource
import io.airbyte.config.ReleaseStage
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.RolloutConfiguration
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.SuggestedStreams
import io.airbyte.config.SupportLevel
import io.airbyte.config.VersionBreakingChange
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionBreakingChanges
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionVersion
import io.airbyte.config.helpers.ConnectorRegistryConverters.toConnectorRollout
import io.airbyte.config.helpers.ConnectorRegistryConverters.toRcDestinationDefinitions
import io.airbyte.config.helpers.ConnectorRegistryConverters.toRcSourceDefinitions
import io.airbyte.config.helpers.ConnectorRegistryConverters.toStandardDestinationDefinition
import io.airbyte.config.helpers.ConnectorRegistryConverters.toStandardSourceDefinition
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.function.ThrowingSupplier
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.util.List
import java.util.UUID

internal class ConnectorRegistryConvertersTest {
  @Test
  fun testConvertRegistrySourceToInternalTypes() {
    val suggestedStreams = SuggestedStreams().withStreams(mutableListOf<String?>("stream1", "stream2"))

    val registrySourceDef =
      ConnectorRegistrySourceDefinition()
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
        .withAbInternal(AbInternal().withSl(200L).withIsEnterprise(true))
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion("doesnt matter")
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withSuggestedStreams(suggestedStreams)
        .withMaxSecondsBetweenMessages(10L)
        .withSupportsFileTransfer(true)
        .withReleases(ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges))

    val stdSourceDef =
      StandardSourceDefinition()
        .withSourceDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withCustom(false)
        .withEnterprise(true)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withMaxSecondsBetweenMessages(10L)

    val actorDefinitionVersion =
      ActorDefinitionVersion()
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
        .withSupportsFileTransfer(true)

    Assertions.assertEquals(stdSourceDef, toStandardSourceDefinition(registrySourceDef))
    Assertions.assertEquals(actorDefinitionVersion, toActorDefinitionVersion(registrySourceDef))
    Assertions.assertEquals(expectedBreakingChanges, toActorDefinitionBreakingChanges(registrySourceDef))
  }

  @Test
  fun testConvertRegistrySourceDefaults() {
    val suggestedStreams = SuggestedStreams().withStreams(mutableListOf<String?>("stream1", "stream2"))
    val registrySourceDef =
      ConnectorRegistrySourceDefinition()
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
        .withReleases(ConnectorReleasesSource().withBreakingChanges(sourceRegistryBreakingChanges))

    val convertedAdv = toActorDefinitionVersion(registrySourceDef)
    Assertions.assertEquals(SupportLevel.NONE, convertedAdv.getSupportLevel())
    Assertions.assertFalse(convertedAdv.getSupportsFileTransfer())
  }

  @Test
  fun testConvertRegistryDestinationToInternalTypes() {
    val registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
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
        .withAbInternal(AbInternal().withSl(200L).withIsEnterprise(true))
        .withReleaseStage(ReleaseStage.GENERALLY_AVAILABLE)
        .withReleaseDate(RELEASE_DATE)
        .withProtocolVersion(PROTOCOL_VERSION)
        .withAllowedHosts(ALLOWED_HOSTS)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)
        .withReleases(ConnectorReleasesDestination().withBreakingChanges(destinationBreakingChanges))
        .withLanguage(LANGUAGE)
        .withSupportsFileTransfer(true)
        .withSupportsDataActivation(true)

    val stdDestinationDef =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(DEF_ID)
        .withName(CONNECTOR_NAME)
        .withTombstone(false)
        .withPublic(true)
        .withEnterprise(true)
        .withCustom(false)
        .withResourceRequirements(RESOURCE_REQUIREMENTS)

    val actorDefinitionVersion =
      ActorDefinitionVersion()
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
        .withSupportsDataActivation(true)

    Assertions.assertEquals(stdDestinationDef, toStandardDestinationDefinition(registryDestinationDef))
    Assertions.assertEquals(actorDefinitionVersion, toActorDefinitionVersion(registryDestinationDef))
    Assertions.assertEquals(expectedBreakingChanges, toActorDefinitionBreakingChanges(registryDestinationDef))
  }

  @Test
  fun testConvertRegistryDestinationDefaults() {
    val registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
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
        .withReleases(ConnectorReleasesDestination().withBreakingChanges(destinationBreakingChanges))

    val convertedAdv = toActorDefinitionVersion(registryDestinationDef)
    Assertions.assertEquals(SupportLevel.NONE, convertedAdv.getSupportLevel())
  }

  @Test
  fun testConvertRegistryDestinationWithoutScopedImpact() {
    val registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
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
        .withReleases(ConnectorReleasesDestination().withBreakingChanges(destinationRegistryBreakingChangesWithoutScopedImpact))

    val actorDefinitionBreakingChanges =
      toActorDefinitionBreakingChanges(registryDestinationDef)
    Assertions.assertEquals(expectedBreakingChanges.size, actorDefinitionBreakingChanges.size)
    for (actorDefinitionBreakingChange in actorDefinitionBreakingChanges) {
      Assertions.assertEquals(actorDefinitionBreakingChange.getScopedImpact(), mutableListOf<Any?>())
    }
  }

  @Test
  fun testParseSourceDefinitionWithNoBreakingChangesReturnsEmptyList() {
    var registrySourceDef: ConnectorRegistrySourceDefinition? = ConnectorRegistrySourceDefinition()
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registrySourceDef), mutableListOf<Any?>())

    registrySourceDef = ConnectorRegistrySourceDefinition().withReleases(ConnectorReleasesSource())
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registrySourceDef), mutableListOf<Any?>())

    registrySourceDef =
      ConnectorRegistrySourceDefinition()
        .withReleases(ConnectorReleasesSource().withBreakingChanges(BreakingChanges()))
        .withSourceDefinitionId(UUID.randomUUID())
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registrySourceDef), mutableListOf<Any?>())
  }

  @Test
  fun testParseDestinationDefinitionWithNoBreakingChangesReturnsEmptyList() {
    var registryDestinationDef: ConnectorRegistryDestinationDefinition? = ConnectorRegistryDestinationDefinition()
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registryDestinationDef), mutableListOf<Any?>())

    registryDestinationDef = ConnectorRegistryDestinationDefinition().withReleases(ConnectorReleasesDestination())
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registryDestinationDef), mutableListOf<Any?>())

    registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
        .withReleases(ConnectorReleasesDestination().withBreakingChanges(BreakingChanges()))
        .withDestinationDefinitionId(UUID.randomUUID())
    Assertions.assertEquals(toActorDefinitionBreakingChanges(registryDestinationDef), mutableListOf<Any?>())
  }

  @Test
  fun testToReleaseCandidateSourceDefinitions() {
    var registrySourceDef = ConnectorRegistrySourceDefinition()
    Assertions.assertEquals(toRcSourceDefinitions(registrySourceDef), mutableListOf<Any?>())

    registrySourceDef =
      ConnectorRegistrySourceDefinition()
        .withReleases(
          ConnectorReleasesSource().withReleaseCandidates(
            ReleaseCandidatesSource().withAdditionalProperty(
              DOCKER_TAG,
              ConnectorRegistrySourceDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY),
            ),
          ),
        )
    var rcDefs = toRcSourceDefinitions(registrySourceDef)
    Assertions.assertEquals(rcDefs.size, 1)
    Assertions.assertEquals(rcDefs.get(0)!!.getDockerImageTag(), DOCKER_TAG)
    Assertions.assertEquals(rcDefs.get(0)!!.getDockerRepository(), DOCKER_REPOSITORY)

    registrySourceDef =
      ConnectorRegistrySourceDefinition()
        .withReleases(
          ConnectorReleasesSource().withReleaseCandidates(
            ReleaseCandidatesSource().withAdditionalProperty(
              DOCKER_TAG,
              null,
            ),
          ),
        )
    rcDefs = toRcSourceDefinitions(registrySourceDef)
    Assertions.assertEquals(rcDefs.size, 0)
  }

  @Test
  fun testToReleaseCandidateDestinationDefinitions() {
    var registryDestinationDef = ConnectorRegistryDestinationDefinition()
    Assertions.assertEquals(toRcDestinationDefinitions(registryDestinationDef), mutableListOf<Any?>())

    registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
        .withReleases(
          ConnectorReleasesDestination().withReleaseCandidates(
            ReleaseCandidatesDestination().withAdditionalProperty(
              DOCKER_TAG,
              ConnectorRegistryDestinationDefinition().withDockerImageTag(DOCKER_TAG).withDockerRepository(DOCKER_REPOSITORY),
            ),
          ),
        )

    var rcDefs =
      toRcDestinationDefinitions(registryDestinationDef)
    Assertions.assertEquals(rcDefs.size, 1)
    Assertions.assertEquals(rcDefs.get(0)!!.getDockerImageTag(), DOCKER_TAG)
    Assertions.assertEquals(rcDefs.get(0)!!.getDockerRepository(), DOCKER_REPOSITORY)

    registryDestinationDef =
      ConnectorRegistryDestinationDefinition()
        .withReleases(
          ConnectorReleasesDestination().withReleaseCandidates(
            ReleaseCandidatesDestination().withAdditionalProperty(
              DOCKER_TAG,
              null,
            ),
          ),
        )
    rcDefs = toRcDestinationDefinitions(registryDestinationDef)
    Assertions.assertEquals(rcDefs.size, 0)
  }

  @Test
  fun testToConnectorRollout() {
    val advId = UUID.randomUUID()
    val initialAdvId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()
    val rolloutConfiguration =
      RolloutConfiguration().withAdvanceDelayMinutes(1L).withInitialPercentage(10L).withMaxPercentage(100L)
    val rcDef =
      ConnectorRegistrySourceDefinition()
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withSourceDefinitionId(actorDefinitionId)
        .withReleases(ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration))
    val rcAdv =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
    val initialAdv =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withVersionId(initialAdvId)
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)

    // Normal behavior
    val rollout = toConnectorRollout(rcDef, rcAdv, initialAdv)

    Assertions.assertEquals(actorDefinitionId, rollout.actorDefinitionId)
    Assertions.assertEquals(rolloutConfiguration.getInitialPercentage().toInt(), rollout.initialRolloutPct)
    Assertions.assertEquals(rolloutConfiguration.getMaxPercentage().toInt(), rollout.finalTargetRolloutPct)
    Assertions.assertEquals(rolloutConfiguration.getAdvanceDelayMinutes().toInt(), rollout.maxStepWaitTimeMins)
    Assertions.assertEquals(ConnectorEnumRolloutState.INITIALIZED, rollout.state)

    // With dockerImageTag mismatch
    val rcDefWithDockerImageTagMismatch =
      ConnectorRegistrySourceDefinition()
        .withDockerImageTag("1.0.0")
        .withDockerRepository(DOCKER_REPOSITORY)
        .withSourceDefinitionId(actorDefinitionId)
        .withReleases(ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration))
    val rcAdvWithDockerImageTagMismatch =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefinitionId)
        .withVersionId(advId)
        .withDockerImageTag("1.1.0")
        .withDockerRepository(DOCKER_REPOSITORY)
    Assertions.assertThrows<AssertionError?>(
      AssertionError::class.java,
      Executable {
        toConnectorRollout(rcDefWithDockerImageTagMismatch, rcAdvWithDockerImageTagMismatch, initialAdv)
      },
    )

    // With definition id mismatch
    val rcDefWithDefinitionIdTagMismatch =
      ConnectorRegistrySourceDefinition()
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
        .withSourceDefinitionId(actorDefinitionId)
        .withReleases(ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration))
    val rcAdvWithDefinitionIdMismatch =
      ActorDefinitionVersion()
        .withActorDefinitionId(advId)
        .withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository(DOCKER_REPOSITORY)
    Assertions.assertThrows<AssertionError?>(
      AssertionError::class.java,
      Executable {
        toConnectorRollout(rcDefWithDefinitionIdTagMismatch, rcAdvWithDefinitionIdMismatch, initialAdv)
      },
    )

    // With docker repository mismatch
    val rcDefDockerRepoMismatch =
      ConnectorRegistrySourceDefinition()
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository("airbyte/source-faker")
        .withSourceDefinitionId(actorDefinitionId)
        .withReleases(ConnectorReleasesSource().withRolloutConfiguration(rolloutConfiguration))
    val rcAdvDockerRepoMismatch =
      ActorDefinitionVersion()
        .withActorDefinitionId(advId)
        .withVersionId(advId)
        .withDockerImageTag(DOCKER_TAG)
        .withDockerRepository("airbyte/source-mismatch")
    Assertions.assertThrows<AssertionError?>(
      AssertionError::class.java,
      Executable {
        toConnectorRollout(rcDefDockerRepoMismatch, rcAdvDockerRepoMismatch, initialAdv)
      },
    )
  }

  @ParameterizedTest
  @CsvSource("0.0.1, true", "dev, true", "test, false", "1.9.1-dev.33a53e6236, true", "97b69a76-1f06-4680-8905-8beda74311d0, false")
  fun testDockerImageValidation(
    dockerImageTag: String?,
    isValid: Boolean,
  ) {
    val registrySourceDefinition =
      ConnectorRegistrySourceDefinition()
        .withDockerImageTag(dockerImageTag)
    val registryDestinationDefinition =
      ConnectorRegistryDestinationDefinition()
        .withDockerImageTag(dockerImageTag)

    if (isValid) {
      Assertions.assertDoesNotThrow<ActorDefinitionVersion?>(ThrowingSupplier { toActorDefinitionVersion(registrySourceDefinition) })
      Assertions.assertDoesNotThrow<ActorDefinitionVersion?>(ThrowingSupplier { toActorDefinitionVersion(registryDestinationDefinition) })
    } else {
      Assertions.assertThrows<IllegalArgumentException?>(
        IllegalArgumentException::class.java,
        Executable { toActorDefinitionVersion(registrySourceDefinition) },
      )
      Assertions.assertThrows<IllegalArgumentException?>(
        IllegalArgumentException::class.java,
        Executable { toActorDefinitionVersion(registryDestinationDefinition) },
      )
    }
  }

  companion object {
    private val DEF_ID: UUID = UUID.randomUUID()
    private const val CONNECTOR_NAME = "postgres"
    private const val DOCKER_REPOSITORY = "airbyte/postgres"
    private const val DOCKER_TAG = "0.1.0"
    private const val DOCS_URL = "https://airbyte.com"
    private const val RELEASE_DATE = "2021-01-01"
    private const val PROTOCOL_VERSION = "1.0.0"
    private const val LANGUAGE = "manifest-only"

    private const val SAMPLE_MESSAGE = "Sample message"

    private const val UPGRADE_DEADLINE = "2023-07-20"

    private const val DEADLINE_ACTION = "upgrade"

    private const val DOCUMENTATION_URL = "https://example.com"
    private val SPEC: ConnectorSpecification? =
      ConnectorSpecification()
        .withConnectionSpecification(
          jsonNode<ImmutableMap<String?, String?>?>(ImmutableMap.of<String?, String?>("key", "val")),
        ).withProtocolVersion(PROTOCOL_VERSION)
    private val ALLOWED_HOSTS: AllowedHosts? = AllowedHosts().withHosts(mutableListOf<String?>("host1", "host2"))
    private val RESOURCE_REQUIREMENTS: ScopedResourceRequirements? =
      ScopedResourceRequirements().withDefault(ResourceRequirements().withCpuRequest("2"))

    private val breakingChangeScope: BreakingChangeScope =
      BreakingChangeScope()
        .withScopeType(BreakingChangeScope.ScopeType.STREAM)
        .withImpactedScopes(mutableListOf<Any?>("stream1", "stream2"))

    private val sourceRegistryBreakingChanges: BreakingChanges? =
      BreakingChanges().withAdditionalProperty(
        PROTOCOL_VERSION,
        VersionBreakingChange()
          .withMessage(SAMPLE_MESSAGE)
          .withUpgradeDeadline(UPGRADE_DEADLINE)
          .withDeadlineAction(DEADLINE_ACTION)
          .withMigrationDocumentationUrl(DOCUMENTATION_URL)
          .withScopedImpact(
            List.of<BreakingChangeScope?>(breakingChangeScope),
          ),
      )

    private val destinationBreakingChanges: BreakingChanges? =
      BreakingChanges().withAdditionalProperty(
        PROTOCOL_VERSION,
        VersionBreakingChange()
          .withMessage(SAMPLE_MESSAGE)
          .withUpgradeDeadline(UPGRADE_DEADLINE)
          .withDeadlineAction(DEADLINE_ACTION)
          .withMigrationDocumentationUrl(DOCUMENTATION_URL)
          .withScopedImpact(
            List.of<BreakingChangeScope?>(breakingChangeScope),
          ),
      )

    private val destinationRegistryBreakingChangesWithoutScopedImpact: BreakingChanges? =
      BreakingChanges().withAdditionalProperty(
        PROTOCOL_VERSION,
        VersionBreakingChange()
          .withMessage(SAMPLE_MESSAGE)
          .withUpgradeDeadline(UPGRADE_DEADLINE)
          .withMigrationDocumentationUrl(DOCUMENTATION_URL),
      )

    private val expectedBreakingChanges =
      List.of<ActorDefinitionBreakingChange?>(
        ActorDefinitionBreakingChange()
          .withActorDefinitionId(DEF_ID)
          .withVersion(Version(PROTOCOL_VERSION))
          .withMigrationDocumentationUrl(DOCUMENTATION_URL)
          .withUpgradeDeadline(UPGRADE_DEADLINE)
          .withDeadlineAction(DEADLINE_ACTION)
          .withMessage(SAMPLE_MESSAGE)
          .withScopedImpact(List.of<BreakingChangeScope?>(breakingChangeScope)),
      )
  }
}
