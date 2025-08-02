/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.helpers

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.StandardSync
import io.airbyte.config.persistence.MockData
import io.airbyte.data.exceptions.InvalidRequestException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.data.services.shared.ConnectorUpdate
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseBreakingChangeScopes
import io.airbyte.featureflag.Workspace
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

internal class ActorDefinitionVersionUpdaterTest {
  private val connectionService = mockk<ConnectionService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>(relaxed = true)
  private val scopedConfigurationService = mockk<ScopedConfigurationService>(relaxed = true)
  private val featureFlagClient = mockk<TestClient>()
  private val connectionTimelineEventService = mockk<ConnectionTimelineEventService>()
  private val actorDefinitionVersionUpdater =
    ActorDefinitionVersionUpdater(
      featureFlagClient,
      connectionService,
      actorDefinitionService,
      scopedConfigurationService,
      connectionTimelineEventService,
    )

  companion object {
    val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()

    val DEFAULT_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag("1.0.0")

    val NEW_VERSION: ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withVersionId(UUID.randomUUID())
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withDockerImageTag("2.0.0")

    val STREAM_SCOPED_BREAKING_CHANGE: ActorDefinitionBreakingChange =
      MockData
        .actorDefinitionBreakingChange(NEW_VERSION.dockerImageTag)!!
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withScopedImpact(
          listOf(
            BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(listOf("affected_stream")),
          ),
        )

    @JvmStatic
    fun getBreakingChangesForUpgradeMethodSource(): Stream<Arguments> =
      Stream.of(
        // Version increases
        Arguments.of("0.0.1", "2.0.0", listOf("1.0.0", "2.0.0")),
        Arguments.of("1.0.0", "1.0.1", listOf<String>()),
        Arguments.of("1.0.0", "1.1.0", listOf<String>()),
        Arguments.of("1.0.1", "1.1.0", listOf<String>()),
        Arguments.of("1.0.0", "2.0.1", listOf("2.0.0")),
        Arguments.of("1.0.1", "2.0.0", listOf("2.0.0")),
        Arguments.of("1.0.0", "2.0.1", listOf("2.0.0")),
        Arguments.of("1.0.1", "2.0.1", listOf("2.0.0")),
        // Version decreases - should never have breaking changes
        Arguments.of("2.0.0", "2.0.0", listOf<String>()),
        Arguments.of("2.0.0", "0.0.1", listOf<String>()),
        Arguments.of("1.0.1", "1.0.0", listOf<String>()),
        Arguments.of("1.1.0", "1.0.0", listOf<String>()),
        Arguments.of("1.1.0", "1.0.1", listOf<String>()),
        Arguments.of("2.0.0", "1.0.0", listOf<String>()),
        Arguments.of("2.0.0", "1.0.1", listOf<String>()),
        Arguments.of("2.0.1", "1.0.0", listOf<String>()),
        Arguments.of("2.0.1", "1.0.1", listOf<String>()),
        Arguments.of("2.0.0", "2.0.0", listOf<String>()),
      )

    @JvmStatic
    fun getBreakingChangesAfterVersionMethodSource(): List<Arguments> =
      listOf(
        Arguments.of("0.1.0", listOf("1.0.0", "2.0.0", "3.0.0")),
        Arguments { arrayOf("1.0.0", listOf("2.0.0", "3.0.0")) },
        Arguments { arrayOf("2.0.0", listOf("3.0.0")) },
        Arguments { arrayOf("3.0.0", listOf<String>()) },
        Arguments { arrayOf("4.0.0", listOf<String>()) },
      )
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testScopedImpactAffectsBreakingChangeImpact(actorIsInBreakingChangeScope: Boolean) {
    every {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
    } returns true

    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID)
    } returns Optional.of(DEFAULT_VERSION)

    val actorId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()

    every {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
    } returns listOf(ActorWorkspaceOrganizationIds(actorId, workspaceId, organizationId))

    every {
      connectionService.actorSyncsAnyListedStream(actorId, listOf("affected_stream"))
    } returns actorIsInBreakingChangeScope

    val configsToWriteSlot = slot<List<ScopedConfiguration>>()
    every {
      scopedConfigurationService.insertScopedConfigurations(capture(configsToWriteSlot))
    } returns listOf()

    actorDefinitionVersionUpdater.updateDefaultVersion(
      ACTOR_DEFINITION_ID,
      NEW_VERSION,
      listOf(STREAM_SCOPED_BREAKING_CHANGE),
    )

    verifyAll {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID)
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
      connectionService.actorSyncsAnyListedStream(actorId, listOf("affected_stream"))
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        listOf(
          ConfigScopeMapWithId(
            actorId,
            mapOf(
              ConfigScopeType.ACTOR to actorId,
              ConfigScopeType.WORKSPACE to workspaceId,
              ConfigScopeType.ORGANIZATION to organizationId,
            ),
          ),
        ),
      )

      // Destination definition should always get the new version
      actorDefinitionService.updateActorDefinitionDefaultVersionId(ACTOR_DEFINITION_ID, NEW_VERSION.versionId)

      if (actorIsInBreakingChangeScope) {
        // Assert pins are created
        scopedConfigurationService.insertScopedConfigurations(any())
        assertEquals(1, configsToWriteSlot.captured.size)

        val capturedConfig = configsToWriteSlot.captured[0]
        assertEquals(
          ScopedConfiguration()
            .withKey(ConnectorVersionKey.key)
            .withValue(DEFAULT_VERSION.versionId.toString())
            .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
            .withResourceId(ACTOR_DEFINITION_ID)
            .withScopeType(ConfigScopeType.ACTOR)
            .withScopeId(actorId)
            .withOriginType(ConfigOriginType.BREAKING_CHANGE)
            .withOrigin(STREAM_SCOPED_BREAKING_CHANGE.version.serialize()),
          capturedConfig.withId(null),
        )
      }
    }

    if (!actorIsInBreakingChangeScope) {
      verify(exactly = 0) {
        scopedConfigurationService.insertScopedConfigurations(any())
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGetActorsAffectedByBreakingChange(useBreakingChangeScopes: Boolean) {
    every {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
    } returns useBreakingChangeScopes

    val actorSyncingAffectedStreamId = UUID.randomUUID()
    val actorNotSyncingAffectedStreamId = UUID.randomUUID()

    every {
      connectionService.actorSyncsAnyListedStream(actorNotSyncingAffectedStreamId, listOf("affected_stream"))
    } returns false

    every {
      connectionService.actorSyncsAnyListedStream(actorSyncingAffectedStreamId, listOf("affected_stream"))
    } returns true

    val actorsAffectedByBreakingChange =
      actorDefinitionVersionUpdater.getActorsAffectedByBreakingChange(
        setOf(actorSyncingAffectedStreamId, actorNotSyncingAffectedStreamId),
        STREAM_SCOPED_BREAKING_CHANGE,
      )

    if (useBreakingChangeScopes) {
      // Affected actors depend on scopes
      assertEquals(setOf(actorSyncingAffectedStreamId), actorsAffectedByBreakingChange)
    } else {
      // All actors are affected by breaking change
      assertEquals(setOf(actorSyncingAffectedStreamId, actorNotSyncingAffectedStreamId), actorsAffectedByBreakingChange)
    }
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  fun testGetBreakingChangesForUpgradeWithActorDefBreakingChanges(
    initialImageTag: String,
    upgradeImageTag: String,
    expectedBreakingChangeVersions: List<String>,
  ) {
    val expectedBreakingChangeVersionsForUpgrade = expectedBreakingChangeVersions.stream().map { version: String -> Version(version) }.toList()
    val breakingChangesForDef =
      listOf(
        ActorDefinitionBreakingChange()
          .withActorDefinitionId(ACTOR_DEFINITION_ID)
          .withVersion(Version("1.0.0"))
          .withMessage("Breaking change 1")
          .withUpgradeDeadline("2021-01-01")
          .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/1.0.0"),
        ActorDefinitionBreakingChange()
          .withActorDefinitionId(ACTOR_DEFINITION_ID)
          .withVersion(Version("2.0.0"))
          .withMessage("Breaking change 2")
          .withUpgradeDeadline("2020-08-09")
          .withMigrationDocumentationUrl("https://docs.airbyte.io/migration-guides/2.0.0"),
      )
    val breakingChangesForUpgrade =
      actorDefinitionVersionUpdater.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef)
    val actualBreakingChangeVersionsForUpgrade =
      breakingChangesForUpgrade
        .stream()
        .map { obj: ActorDefinitionBreakingChange -> obj.version }
        .toList()
    assertEquals(expectedBreakingChangeVersionsForUpgrade.size, actualBreakingChangeVersionsForUpgrade.size)
    assertTrue(actualBreakingChangeVersionsForUpgrade.containsAll(expectedBreakingChangeVersionsForUpgrade))
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesForUpgradeMethodSource")
  fun testGetBreakingChangesForUpgradeWithNoActorDefinitionBreakingChanges(
    initialImageTag: String,
    upgradeImageTag: String,
    expectedBreakingChangeVersions: List<String>,
  ) {
    val breakingChangesForDef = listOf<ActorDefinitionBreakingChange>()
    assertTrue(actorDefinitionVersionUpdater.getBreakingChangesForUpgrade(initialImageTag, upgradeImageTag, breakingChangesForDef).isEmpty())
  }

  @Test
  fun testUpgradeActorVersionWithBCPin() {
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()

    val breakingChangePinConfig =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withOriginType(ConfigOriginType.BREAKING_CHANGE)
        .withValue(UUID.randomUUID().toString())

    every {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )
    } returns Optional.of(breakingChangePinConfig)

    val oldVersionTag = "some-old-version"
    val newVersionTag = "new-version"
    every {
      actorDefinitionService.getActorDefinitionVersion(UUID.fromString(breakingChangePinConfig.value))
    } returns ActorDefinitionVersion().withDockerImageTag(oldVersionTag)
    every {
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
    } returns Optional.of(ActorDefinitionVersion().withDockerImageTag(newVersionTag))

    val connectionId = UUID.randomUUID()
    every {
      connectionService.listConnectionsBySource(actorId, true)
    } returns listOf(StandardSync().withConnectionId(connectionId))
    actorDefinitionVersionUpdater.upgradeActorVersion(actorId, actorDefinitionId, ActorType.SOURCE, "SourceMySql")

    verifyAll {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )

      scopedConfigurationService.deleteScopedConfiguration(breakingChangePinConfig.id)
      connectionTimelineEventService.writeEvent(
        connectionId,
        ConnectorUpdate(
          oldVersionTag,
          newVersionTag,
          ConnectorUpdate.ConnectorType.SOURCE,
          "SourceMySql",
          ConnectorUpdate.UpdateType.BREAKING_CHANGE_MANUAL.name,
        ),
        null,
      )
    }
  }

  @Test
  fun testUpgradeActorVersionWithManualPinThrowsError() {
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()

    val manualPinConfig =
      ScopedConfiguration()
        .withId(UUID.randomUUID())
        .withOriginType(ConfigOriginType.USER)

    every {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )
    } returns Optional.of(manualPinConfig)

    assertThrows<RuntimeException> {
      actorDefinitionVersionUpdater.upgradeActorVersion(actorId, actorDefinitionId, ActorType.SOURCE, "")
    }

    verifyAll {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )
    }
  }

  @Test
  fun testUpgradeActorVersionWithNoPins() {
    val actorId = UUID.randomUUID()
    val actorDefinitionId = UUID.randomUUID()

    every {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )
    } returns Optional.empty()

    actorDefinitionVersionUpdater.upgradeActorVersion(actorId, actorDefinitionId, ActorType.SOURCE, "")

    verifyAll {
      scopedConfigurationService.getScopedConfiguration(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigScopeType.ACTOR,
        actorId,
      )
    }
  }

  @Test
  fun testProcessBreakingChangesForUpgrade() {
    every {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
    } returns true

    val pinnedActorId = UUID.randomUUID()
    val withImpactedStreamActorId = UUID.randomUUID()
    val noImpactedStreamActorId = UUID.randomUUID()
    val noImpactedStreamActorId2 = UUID.randomUUID()

    val actors =
      listOf(
        ActorWorkspaceOrganizationIds(pinnedActorId, UUID.randomUUID(), UUID.randomUUID()),
        ActorWorkspaceOrganizationIds(withImpactedStreamActorId, UUID.randomUUID(), UUID.randomUUID()),
        ActorWorkspaceOrganizationIds(noImpactedStreamActorId, UUID.randomUUID(), UUID.randomUUID()),
        ActorWorkspaceOrganizationIds(noImpactedStreamActorId2, UUID.randomUUID(), UUID.randomUUID()),
      )

    val currentVersion = DEFAULT_VERSION
    val limitedScopeBreakingChange = STREAM_SCOPED_BREAKING_CHANGE
    val breakingChange = MockData.actorDefinitionBreakingChange("3.0.0")!!
    val breakingChangesForUpgrade = listOf(limitedScopeBreakingChange, breakingChange)

    every {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
    } returns actors

    val scopeMaps = actors.map { idsToConfigScopeMap(it) }

    // Setup: as we process the breaking changes, the pinned actors returned will include actors pinned due to the prior breaking change
    every {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        scopeMaps,
      )
    } returnsMany
      listOf(
        mapOf(
          pinnedActorId to ScopedConfiguration(),
        ),
        mapOf(
          pinnedActorId to ScopedConfiguration(),
          withImpactedStreamActorId to ScopedConfiguration(),
        ),
        mapOf(
          pinnedActorId to ScopedConfiguration(),
          withImpactedStreamActorId to ScopedConfiguration(),
          noImpactedStreamActorId to ScopedConfiguration(),
          noImpactedStreamActorId2 to ScopedConfiguration(),
        ),
      )

    // Setup: For the limited-impact breaking change, only mock that the targeted actor is syncing the affected stream
    every {
      connectionService.actorSyncsAnyListedStream(any(), any())
    } returns false

    every {
      connectionService.actorSyncsAnyListedStream(withImpactedStreamActorId, listOf("affected_stream"))
    } returns true

    // Collect written configs to perform assertions
    val capturedConfigsToWrite = mutableListOf<List<ScopedConfiguration>>()
    every {
      scopedConfigurationService.insertScopedConfigurations(capture(capturedConfigsToWrite))
    } returns listOf()

    // Act: call method under test
    actorDefinitionVersionUpdater.processBreakingChangesForUpgrade(
      currentVersion,
      breakingChangesForUpgrade,
    )

    verify {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
      connectionService.actorSyncsAnyListedStream(noImpactedStreamActorId, listOf("affected_stream"))
    }

    // Assert: we get pinned actors and insert new pins for each processed breaking change (2)
    verify(exactly = 2) {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        scopeMaps,
      )
      scopedConfigurationService.insertScopedConfigurations(any())
    }

    assertEquals(2, capturedConfigsToWrite.size)

    // Assert: limited-impact breaking change should pin the actor with the affected stream
    val configsForScopedBC = capturedConfigsToWrite[0]
    assertEquals(1, configsForScopedBC.size)
    val expectedConfig1 = buildBreakingChangeScopedConfig(withImpactedStreamActorId, limitedScopeBreakingChange)
    assertEquals(expectedConfig1, configsForScopedBC[0].withId(null))

    // Assert: breaking change should pin all remaining unpinned actors
    val configsForGlobalBC = capturedConfigsToWrite[1].sortedBy { it.scopeId }
    assertEquals(2, configsForGlobalBC.size)
    val sortedExpectedIds = listOf(noImpactedStreamActorId, noImpactedStreamActorId2).sorted()
    val expectedConfig2 = buildBreakingChangeScopedConfig(sortedExpectedIds[0], breakingChange)
    assertEquals(expectedConfig2, configsForGlobalBC[0].withId(null))

    val expectedConfig3 = buildBreakingChangeScopedConfig(sortedExpectedIds[1], breakingChange)
    assertEquals(expectedConfig3, configsForGlobalBC[1].withId(null))
  }

  @Test
  fun `testGetUpgradeCandidates`() {
    val pinnedActorId = UUID.randomUUID()
    val pinnedActorId2 = UUID.randomUUID()
    val unpinnedActorId = UUID.randomUUID()
    val unpinnedActorId2 = UUID.randomUUID()
    val configScopeMaps =
      listOf(
        ConfigScopeMapWithId(pinnedActorId, mapOf()),
        ConfigScopeMapWithId(pinnedActorId2, mapOf()),
        ConfigScopeMapWithId(unpinnedActorId, mapOf()),
        ConfigScopeMapWithId(unpinnedActorId2, mapOf()),
      )

    every {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        configScopeMaps,
      )
    } returns
      mapOf(
        pinnedActorId to ScopedConfiguration(),
        pinnedActorId2 to ScopedConfiguration(),
      )

    val upgradeCandidates = actorDefinitionVersionUpdater.getUpgradeCandidates(ACTOR_DEFINITION_ID, configScopeMaps)

    assertEquals(2, upgradeCandidates.size)
    assertEquals(setOf(unpinnedActorId, unpinnedActorId2), upgradeCandidates)

    verifyAll {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        configScopeMaps,
      )
    }
  }

  @Test
  fun testCreateBreakingChangePinsForActors() {
    val actorIds = setOf(UUID.randomUUID(), UUID.randomUUID())

    val scopedConfigsCapture = slot<List<ScopedConfiguration>>()

    every {
      scopedConfigurationService.insertScopedConfigurations(capture(scopedConfigsCapture))
    } returns listOf()

    actorDefinitionVersionUpdater.createBreakingChangePinsForActors(actorIds, DEFAULT_VERSION, STREAM_SCOPED_BREAKING_CHANGE)

    verify(exactly = 1) {
      scopedConfigurationService.insertScopedConfigurations(any())
    }

    assertEquals(2, scopedConfigsCapture.captured.size)
    for (actorId in actorIds) {
      val capturedConfig = scopedConfigsCapture.captured.find { it.scopeId == actorId }
      assertNotNull(capturedConfig)
      assertNotNull(capturedConfig!!.id)

      val expectedConfig = buildBreakingChangeScopedConfig(actorId, STREAM_SCOPED_BREAKING_CHANGE)
      assertEquals(expectedConfig, capturedConfig.withId(null))
    }
  }

  @Test
  fun testCreateReleaseCandidatePinsForActors() {
    val eligibleButNotPinnedActorId = UUID.randomUUID()
    val actors =
      listOf(
        ActorWorkspaceOrganizationIds(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()),
        ActorWorkspaceOrganizationIds(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()),
      )

    every {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
    } returns actors

    val allEligible = actors + listOf(ActorWorkspaceOrganizationIds(eligibleButNotPinnedActorId, UUID.randomUUID(), UUID.randomUUID()))
    val scopeMaps = allEligible.map { idsToConfigScopeMap(it) }

    // Setup: no actors are pinned for the release candidate
    every {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        scopeMaps,
      )
    } returns emptyMap()

    // Collect written configs to perform assertions
    val capturedConfigsToWrite = mutableListOf<List<ScopedConfiguration>>()
    every {
      scopedConfigurationService.insertScopedConfigurations(capture(capturedConfigsToWrite))
    } returns listOf()

    // Act: call method under test
    actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(
      actors.map { it.actorId }.toSet(),
      ACTOR_DEFINITION_ID,
      DEFAULT_VERSION.versionId,
      UUID.randomUUID(),
    )

    // Assert: we've pinned 2 actors for the release candidate
    verify {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        any(),
      )
      scopedConfigurationService.insertScopedConfigurations(any())
    }

    assertEquals(2, capturedConfigsToWrite.first().size)
  }

  @Test
  fun testCreateReleaseCandidatePinsWhenActorIsAlreadyPinned() {
    val pinnedActorId = UUID.randomUUID()
    val ineligibleAndNotPinnedActorId = UUID.randomUUID()
    val actors =
      listOf(
        ActorWorkspaceOrganizationIds(pinnedActorId, UUID.randomUUID(), UUID.randomUUID()),
        ActorWorkspaceOrganizationIds(ineligibleAndNotPinnedActorId, UUID.randomUUID(), UUID.randomUUID()),
      )

    every {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
    } returns actors

    val scopeMaps = actors.map { idsToConfigScopeMap(it) }

    // Setup: we return the pinned actor
    every {
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        scopeMaps,
      )
    } returnsMany
      listOf(
        mapOf(
          pinnedActorId to ScopedConfiguration(),
        ),
      )

    // Collect written configs to perform assertions
    val capturedConfigsToWrite = mutableListOf<List<ScopedConfiguration>>()
    every {
      scopedConfigurationService.insertScopedConfigurations(capture(capturedConfigsToWrite))
    } returns listOf()

    // Act: call method under test
    assertThrows<InvalidRequestException> {
      actorDefinitionVersionUpdater.createReleaseCandidatePinsForActors(
        actors.map { it.actorId }.toSet(),
        ACTOR_DEFINITION_ID,
        DEFAULT_VERSION.versionId,
        UUID.randomUUID(),
      )
    }

    verify {
      actorDefinitionService.getActorIdsForDefinition(ACTOR_DEFINITION_ID)
      scopedConfigurationService.getScopedConfigurations(
        ConnectorVersionKey,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        scopeMaps,
      )
    }

    // Assert: no actors are pinned
    verify(exactly = 0) {
      scopedConfigurationService.insertScopedConfigurations(any())
    }
    assertEquals(0, capturedConfigsToWrite.size)
  }

  @Test
  fun testUnpinReleaseCandidatesVersion() {
    val actorIds = setOf(UUID.randomUUID(), UUID.randomUUID())
    val releaseCandidateVersionId = UUID.randomUUID()
    val scopedConfigurationsToDelete = actorIds.map { buildReleaseCandidateScopedConfig(it, DEFAULT_VERSION.versionId, releaseCandidateVersionId) }

    every {
      scopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        listOf(releaseCandidateVersionId.toString()),
      )
    } returns scopedConfigurationsToDelete

    actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(ACTOR_DEFINITION_ID, releaseCandidateVersionId)

    verifyAll {
      scopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigScopeType.ACTOR,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        listOf(releaseCandidateVersionId.toString()),
      )

      scopedConfigurationService.deleteScopedConfigurations(scopedConfigurationsToDelete.map { it.id })
    }
  }

  @Test
  fun testUnpinReleaseCandidatesVersionWithNoRCsToUnpin() {
    actorDefinitionVersionUpdater.removeReleaseCandidatePinsForVersion(ACTOR_DEFINITION_ID, UUID.randomUUID())

    verify {
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
    }
    verify(exactly = 0) {
      scopedConfigurationService.deleteScopedConfigurations(any())
    }
  }

  @Test
  fun testMigrateReleaseCandidatePins() {
    val actorDefinitionId = UUID.randomUUID()
    val origins = listOf("origin1", "origin2")
    val newOrigin = "origin3"
    val newReleaseCandidateVersionId = UUID.randomUUID()

    every {
      scopedConfigurationService.updateScopedConfigurationsOriginAndValuesForOriginInList(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        origins,
        newOrigin,
        newReleaseCandidateVersionId.toString(),
      )
    } just Runs

    actorDefinitionVersionUpdater.migrateReleaseCandidatePins(
      actorDefinitionId,
      origins,
      newOrigin,
      newReleaseCandidateVersionId,
    )

    verify {
      scopedConfigurationService.updateScopedConfigurationsOriginAndValuesForOriginInList(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        actorDefinitionId,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        origins,
        newOrigin,
        newReleaseCandidateVersionId.toString(),
      )
    }
  }

  @ParameterizedTest
  @MethodSource("getBreakingChangesAfterVersionMethodSource")
  fun testGetBreakingChangesAfterVersion(
    versionTag: String,
    expectedBreakingChanges: List<String>,
  ) {
    val breakingChanges =
      listOf(
        MockData.actorDefinitionBreakingChange("1.0.0")!!,
        MockData.actorDefinitionBreakingChange("2.0.0")!!,
        MockData.actorDefinitionBreakingChange("3.0.0")!!,
      )

    val actualBreakingChanges =
      actorDefinitionVersionUpdater
        .getBreakingChangesAfterVersion(
          versionTag,
          breakingChanges,
        ).map { it.version.serialize() }
        .toList()

    assertEquals(expectedBreakingChanges, actualBreakingChanges)
  }

  @Test
  fun testGetBreakingChangesAfterVersionWithNoBreakingChanges() {
    val actualBreakingChanges =
      actorDefinitionVersionUpdater.getBreakingChangesAfterVersion(
        "1.0.0",
        listOf(),
      )

    assertEquals(listOf<ActorDefinitionBreakingChange>(), actualBreakingChanges)
  }

  @Test
  fun testProcessBreakingChangePinRollbacks() {
    val oldBC = MockData.actorDefinitionBreakingChange("1.0.0")!!
    val currentVersionBC = MockData.actorDefinitionBreakingChange("2.0.0")!!
    val rolledBackBC = MockData.actorDefinitionBreakingChange("3.0.0")!!

    val allBreakingChanges = listOf(oldBC, currentVersionBC, rolledBackBC)
    val idsPinnedForV3 = listOf(UUID.randomUUID(), UUID.randomUUID())

    every {
      scopedConfigurationService.listScopedConfigurationsWithOrigins(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigOriginType.BREAKING_CHANGE,
        listOf(rolledBackBC.version.serialize()),
      )
    } returns idsPinnedForV3.map { buildBreakingChangeScopedConfig(it, rolledBackBC).withId(it) }

    actorDefinitionVersionUpdater.processBreakingChangePinRollbacks(ACTOR_DEFINITION_ID, NEW_VERSION, allBreakingChanges)

    verifyAll {
      scopedConfigurationService.listScopedConfigurationsWithOrigins(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        ACTOR_DEFINITION_ID,
        ConfigOriginType.BREAKING_CHANGE,
        listOf(rolledBackBC.version.serialize()),
      )

      scopedConfigurationService.deleteScopedConfigurations(idsPinnedForV3)
    }
  }

  @Test
  fun testProcessBreakingChangePinRollbacksWithNoBCsToRollBack() {
    val breakingChanges =
      listOf(
        MockData.actorDefinitionBreakingChange("1.0.0")!!,
        MockData.actorDefinitionBreakingChange("2.0.0")!!,
      )

    actorDefinitionVersionUpdater.processBreakingChangePinRollbacks(ACTOR_DEFINITION_ID, NEW_VERSION, breakingChanges)

    verify(exactly = 0) {
      scopedConfigurationService.listScopedConfigurationsWithOrigins(any(), any(), any(), any(), any())
      scopedConfigurationService.deleteScopedConfigurations(any())
    }
  }

  private fun buildBreakingChangeScopedConfig(
    actorId: UUID,
    breakingChange: ActorDefinitionBreakingChange,
  ): ScopedConfiguration =
    ScopedConfiguration()
      .withKey(ConnectorVersionKey.key)
      .withValue(DEFAULT_VERSION.versionId.toString())
      .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
      .withResourceId(ACTOR_DEFINITION_ID)
      .withScopeType(ConfigScopeType.ACTOR)
      .withScopeId(actorId)
      .withOriginType(ConfigOriginType.BREAKING_CHANGE)
      .withOrigin(breakingChange.version.serialize())

  private fun buildReleaseCandidateScopedConfig(
    actorId: UUID,
    defaultVersionId: UUID,
    releaseCandidateVersionId: UUID,
  ): ScopedConfiguration =
    ScopedConfiguration()
      .withKey(ConnectorVersionKey.key)
      .withValue(releaseCandidateVersionId.toString())
      .withResourceType(ConfigResourceType.ACTOR_DEFINITION)
      .withResourceId(ACTOR_DEFINITION_ID)
      .withScopeType(ConfigScopeType.ACTOR)
      .withScopeId(actorId)
      .withOriginType(ConfigOriginType.CONNECTOR_ROLLOUT)
      .withOrigin(defaultVersionId.toString())

  private fun idsToConfigScopeMap(awoIds: ActorWorkspaceOrganizationIds): ConfigScopeMapWithId =
    ConfigScopeMapWithId(
      awoIds.actorId,
      mapOf(
        ConfigScopeType.ACTOR to awoIds.actorId,
        ConfigScopeType.WORKSPACE to awoIds.workspaceId,
        ConfigScopeType.ORGANIZATION to awoIds.organizationId,
      ),
    )
}
