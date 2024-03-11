package io.airbyte.data.helpers

import io.airbyte.commons.version.Version
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.BreakingChangeScope
import io.airbyte.config.persistence.MockData
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.UseBreakingChangeScopes
import io.airbyte.featureflag.Workspace
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verifyAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
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
  private val featureFlagClient = mockk<TestClient>()
  private val actorDefinitionVersionUpdater = ActorDefinitionVersionUpdater(featureFlagClient, connectionService, actorDefinitionService)

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
      MockData.actorDefinitionBreakingChange(NEW_VERSION.dockerImageTag)
        .withActorDefinitionId(ACTOR_DEFINITION_ID)
        .withScopedImpact(
          listOf(
            BreakingChangeScope().withScopeType(BreakingChangeScope.ScopeType.STREAM).withImpactedScopes(listOf("affected_stream")),
          ),
        )

    @JvmStatic
    fun getBreakingChangesForUpgradeMethodSource(): Stream<Arguments> {
      return Stream.of(
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
    }
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
    every {
      actorDefinitionService.getActorsWithDefaultVersionId(DEFAULT_VERSION.versionId)
    } returns setOf(actorId)

    every {
      connectionService.actorSyncsAnyListedStream(actorId, listOf("affected_stream"))
    } returns actorIsInBreakingChangeScope

    actorDefinitionVersionUpdater.updateDefaultVersion(
      ACTOR_DEFINITION_ID,
      NEW_VERSION,
      listOf(STREAM_SCOPED_BREAKING_CHANGE),
    )

    verifyAll {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
      actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(ACTOR_DEFINITION_ID)
      actorDefinitionService.getActorsWithDefaultVersionId(DEFAULT_VERSION.versionId)
      connectionService.actorSyncsAnyListedStream(actorId, listOf("affected_stream"))

      // Destination definition should always get the new version
      actorDefinitionService.updateActorDefinitionDefaultVersionId(ACTOR_DEFINITION_ID, NEW_VERSION.versionId)

      if (actorIsInBreakingChangeScope) {
        // Assert actor is not updated
        actorDefinitionService.setActorDefaultVersions(listOf(), NEW_VERSION.versionId)
      } else {
        // Assert actor is upgraded to the new version
        actorDefinitionService.setActorDefaultVersions(listOf(actorId), NEW_VERSION.versionId)
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGetActorsForNonBreakingUpgrade(useBreakingChangeScopes: Boolean) {
    every {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
    } returns useBreakingChangeScopes

    val actorIdOnInitialVersion = UUID.randomUUID()
    every {
      actorDefinitionService.getActorsWithDefaultVersionId(DEFAULT_VERSION.versionId)
    } returns setOf(actorIdOnInitialVersion)

    val actorsToUpgrade = actorDefinitionVersionUpdater.getActorsToUpgrade(DEFAULT_VERSION, NEW_VERSION, listOf())

    // All actors should get upgraded
    assertEquals(setOf(actorIdOnInitialVersion), actorsToUpgrade)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testGetActorsForBreakingUpgrade(useBreakingChangeScopes: Boolean) {
    every {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
    } returns useBreakingChangeScopes

    // Set up an actor that syncs an affected stream
    val actorNotSyncingAffectedStream = UUID.randomUUID()
    val actorSyncingAffectedStream = UUID.randomUUID()

    every {
      actorDefinitionService.getActorsWithDefaultVersionId(DEFAULT_VERSION.versionId)
    } returns setOf(actorSyncingAffectedStream, actorNotSyncingAffectedStream)

    every {
      connectionService.actorSyncsAnyListedStream(actorSyncingAffectedStream, listOf("affected_stream"))
    } returns true

    every {
      connectionService.actorSyncsAnyListedStream(actorNotSyncingAffectedStream, listOf("affected_stream"))
    } returns false

    val actorsToUpgrade =
      actorDefinitionVersionUpdater.getActorsToUpgrade(
        DEFAULT_VERSION,
        NEW_VERSION,
        listOf(STREAM_SCOPED_BREAKING_CHANGE),
      )

    if (useBreakingChangeScopes) {
      // Unaffected actors will be upgraded
      assertEquals(setOf(actorNotSyncingAffectedStream), actorsToUpgrade)
    } else {
      // No actors will be upgraded
      assertEquals(setOf<UUID>(), actorsToUpgrade)
    }

    verifyAll {
      featureFlagClient.boolVariation(UseBreakingChangeScopes, Workspace(ANONYMOUS))
      actorDefinitionService.getActorsWithDefaultVersionId(DEFAULT_VERSION.versionId)
      if (useBreakingChangeScopes) {
        connectionService.actorSyncsAnyListedStream(actorSyncingAffectedStream, listOf("affected_stream"))
        connectionService.actorSyncsAnyListedStream(actorNotSyncingAffectedStream, listOf("affected_stream"))
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
      breakingChangesForUpgrade.stream().map { obj: ActorDefinitionBreakingChange -> obj.version }
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
}
