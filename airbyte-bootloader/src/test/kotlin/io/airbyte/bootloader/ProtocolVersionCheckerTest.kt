/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Optional
import java.util.UUID

private val V0_0_0 = Version("0.0.0")
private val V1_0_0 = Version("1.0.0")
private val V2_0_0 = Version("2.0.0")

internal class ProtocolVersionCheckerTest {
  var definitionsProvider: DefinitionsProvider = mockk()
  var jobPersistence: JobPersistence = mockk()
  var actorDefinitionService: ActorDefinitionService = mockk()
  var sourceService: SourceService = mockk()
  var destinationService: DestinationService = mockk()

  @BeforeEach
  fun beforeEach() {
    clearAllMocks()
    every { jobPersistence?.getVersion() } returns Optional.of("1.2.3")
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testFirstInstallCheck(supportAutoUpgrade: Boolean) {
    val expectedRange = AirbyteProtocolVersionRange(V0_0_0, V1_0_0)
    every { jobPersistence.getVersion() } returns Optional.empty()
    every { jobPersistence.getCurrentProtocolVersionRange() } returns Optional.empty()
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val supportedRange = protocolVersionChecker.validate(supportAutoUpgrade)
    Assertions.assertNotNull(supportedRange)
    Assertions.assertEquals(expectedRange.max, supportedRange?.max)
    Assertions.assertEquals(expectedRange.min, supportedRange?.min)
  }

  @Test
  fun testGetTargetRange() {
    val expectedRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    Assertions.assertEquals(expectedRange.max, protocolVersionChecker.targetProtocolVersionRange.max)
    Assertions.assertEquals(expectedRange.min, protocolVersionChecker.targetProtocolVersionRange.min)
  }

  @Test
  fun testRetrievingCurrentConflicts() {
    val targetRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val source3 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()

    val initialActorDefinitions =
      mapOf(
        source1 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        source2 to (ActorType.SOURCE to V1_0_0).toMapEntry(),
        source3 to (ActorType.SOURCE to V2_0_0).toMapEntry(),
        dest1 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
        dest2 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
      )
    every {
      actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    } returns initialActorDefinitions

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        targetRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val conflicts: Map<ActorType, Set<UUID>> = protocolVersionChecker.getConflictingActorDefinitions(targetRange)

    val expectedConflicts =
      mapOf(
        ActorType.DESTINATION to setOf(dest1, dest2),
        ActorType.SOURCE to setOf(source1),
      )
    Assertions.assertEquals(expectedConflicts, conflicts)
  }

  @Test
  fun testRetrievingCurrentConflictsWhenNoConflicts() {
    val targetRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val source1 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()

    val initialActorDefinitions =
      mapOf(
        source1 to (ActorType.SOURCE to V2_0_0).toMapEntry(),
        dest1 to (ActorType.DESTINATION to V1_0_0).toMapEntry(),
      )
    every {
      actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    } returns initialActorDefinitions

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        targetRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val conflicts: Map<ActorType, Set<UUID>> = protocolVersionChecker.getConflictingActorDefinitions(targetRange)

    Assertions.assertEquals(java.util.Map.of<Any, Any>(), conflicts)
  }

  @Test
  fun testProjectRemainingSourceConflicts() {
    val targetRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val unrelatedSource = UUID.randomUUID()
    val upgradedSource = UUID.randomUUID()
    val notChangedSource = UUID.randomUUID()
    val missingSource = UUID.randomUUID()
    val initialConflicts = java.util.Set.of(upgradedSource, notChangedSource, missingSource)

    setNewSourceDefinitions(
      listOf(
        Pair(unrelatedSource, V2_0_0).toMapEntry(),
        Pair(upgradedSource, V1_0_0).toMapEntry(),
        Pair(notChangedSource, V0_0_0).toMapEntry(),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        targetRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actualConflicts =
      protocolVersionChecker.projectRemainingConflictsAfterConnectorUpgrades(targetRange, initialConflicts, ActorType.SOURCE)

    val expectedConflicts = java.util.Set.of(notChangedSource, missingSource)
    Assertions.assertEquals(expectedConflicts, actualConflicts)
  }

  @Test
  fun testProjectRemainingDestinationConflicts() {
    val targetRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()
    val dest3 = UUID.randomUUID()
    val initialConflicts = java.util.Set.of(dest1, dest2, dest3)

    setNewDestinationDefinitions(
      listOf(
        Pair(dest1, V2_0_0).toMapEntry(),
        Pair(dest2, V1_0_0).toMapEntry(),
        Pair(dest3, V2_0_0).toMapEntry(),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        targetRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actualConflicts =
      protocolVersionChecker.projectRemainingConflictsAfterConnectorUpgrades(targetRange, initialConflicts, ActorType.DESTINATION)

    val expectedConflicts = setOf<UUID>()
    Assertions.assertEquals(expectedConflicts, actualConflicts)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testValidateSameRange(supportAutoUpgrade: Boolean) {
    val expectedRange = AirbyteProtocolVersionRange(V0_0_0, V2_0_0)
    setCurrentProtocolRangeRange(expectedRange.min, expectedRange.max)
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )

    val supportedRange = protocolVersionChecker.validate(supportAutoUpgrade)
    Assertions.assertNotNull(supportedRange)
    Assertions.assertEquals(expectedRange.max, supportedRange?.max)
    Assertions.assertEquals(expectedRange.min, supportedRange?.min)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testValidateAllConnectorsAreUpgraded(supportAutoUpgrade: Boolean) {
    setCurrentProtocolRangeRange(V0_0_0, V1_0_0)

    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val source3 = UUID.randomUUID()
    val source4 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()
    val dest3 = UUID.randomUUID()
    val expectedTargetVersionRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val initialActorDefinitions =
      mapOf(
        source1 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        source2 to (ActorType.SOURCE to V1_0_0).toMapEntry(),
        source3 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        source4 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        dest1 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
        dest2 to (ActorType.DESTINATION to V1_0_0).toMapEntry(),
        dest3 to (ActorType.DESTINATION to V2_0_0).toMapEntry(),
      )
    every {
      actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    } returns initialActorDefinitions

    setNewSourceDefinitions(
      listOf(
        Pair(source1, V1_0_0).toMapEntry(),
        Pair(source2, V1_0_0).toMapEntry(),
        Pair(source3, V2_0_0).toMapEntry(),
        Pair(source4, V1_0_0).toMapEntry(),
      ),
    )
    setNewDestinationDefinitions(
      listOf(
        Pair(dest1, V1_0_0).toMapEntry(),
        Pair(dest2, V1_0_0).toMapEntry(),
        Pair(dest3, V2_0_0).toMapEntry(),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedTargetVersionRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actualRange = protocolVersionChecker.validate(supportAutoUpgrade)

    // Without auto upgrade, we will fail the validation because it would require connector automatic
    // actor definition
    // upgrade for used sources/destinations.
    if (supportAutoUpgrade) {
      Assertions.assertNotNull(actualRange)
      Assertions.assertEquals(expectedTargetVersionRange.max, actualRange?.max)
      Assertions.assertEquals(expectedTargetVersionRange.min, actualRange?.min)
    } else {
      Assertions.assertNull(actualRange)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testValidateBadUpgradeMissingSource(supportAutoUpgrade: Boolean) {
    val expectedTargetVersionRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)
    setCurrentProtocolRangeRange(V0_0_0, V1_0_0)

    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()

    val initialActorDefinitions =
      mapOf(
        source1 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        source2 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        dest1 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
        dest2 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
      )
    every {
      actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    } returns initialActorDefinitions

    setNewSourceDefinitions(
      listOf(
        Pair(source1, V0_0_0).toMapEntry(),
        Pair(source2, V1_0_0).toMapEntry(),
      ),
    )
    setNewDestinationDefinitions(
      listOf(
        Pair(dest1, V1_0_0).toMapEntry(),
        Pair(dest2, V1_0_0).toMapEntry(),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedTargetVersionRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actualRange = protocolVersionChecker.validate(supportAutoUpgrade)
    Assertions.assertNull(actualRange)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testValidateBadUpgradeMissingDestination(supportAutoUpgrade: Boolean) {
    val expectedTargetVersionRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)
    setCurrentProtocolRangeRange(V0_0_0, V1_0_0)

    val source1 = UUID.randomUUID()
    val source2 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()
    val dest2 = UUID.randomUUID()

    val initialActorDefinitions =
      mapOf(
        source1 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        source2 to (ActorType.SOURCE to V0_0_0).toMapEntry(),
        dest1 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
        dest2 to (ActorType.DESTINATION to V0_0_0).toMapEntry(),
      )
    every {
      actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    } returns initialActorDefinitions

    setNewSourceDefinitions(
      java.util.List.of(
        java.util.Map.entry(source1, V1_0_0),
        java.util.Map.entry(source2, V1_0_0),
      ),
    )
    setNewDestinationDefinitions(
      java.util.List.of(
        java.util.Map.entry(dest1, V1_0_0),
        java.util.Map.entry(dest2, V0_0_0),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence,
        expectedTargetVersionRange,
        actorDefinitionService,
        definitionsProvider,
        sourceService,
        destinationService,
      )
    val actualRange = protocolVersionChecker.validate(supportAutoUpgrade)
    Assertions.assertNull(actualRange)
  }

  private fun setCurrentProtocolRangeRange(
    min: Version,
    max: Version,
  ) {
    every { jobPersistence.getCurrentProtocolVersionRange() } returns Optional.of(AirbyteProtocolVersionRange(min, max))
    every { jobPersistence.getAirbyteProtocolVersionMin() } returns Optional.of(min)
    every { jobPersistence.getAirbyteProtocolVersionMax() } returns Optional.of(max)
  }

  private fun setNewDestinationDefinitions(defs: List<Map.Entry<UUID, Version>>) {
    val destDefinitions =
      defs
        .map { e: Map.Entry<UUID, Version> ->
          ConnectorRegistryDestinationDefinition()
            .withDestinationDefinitionId(e.key)
            .withSpec(ConnectorSpecification().withProtocolVersion(e.value.serialize()))
        }.toList()
    every { definitionsProvider.getDestinationDefinitions() } returns destDefinitions
  }

  private fun setNewSourceDefinitions(defs: List<Map.Entry<UUID, Version>>) {
    val sourceDefinitions =
      defs
        .map { e: Map.Entry<UUID, Version> ->
          ConnectorRegistrySourceDefinition()
            .withSourceDefinitionId(e.key)
            .withSpec(ConnectorSpecification().withProtocolVersion(e.value.serialize()))
        }.toList()
    every { definitionsProvider.getSourceDefinitions() } returns sourceDefinitions
  }
}

/**
 * Extension function for converting a `Pair` into a `Map.Entry`.
 *
 * This function maps a `Pair` of two elements to a `Map.Entry` object using the `java.util.Map.entry` function,
 * where the first component of the pair is used as the key and the second component is used as the value.
 *
 * Necessary because [ActorDefinitionService.getActorDefinitionToProtocolVersionMap] returns a Map<X, Map.Entry<>>
 *
 * @receiver The `Pair` instance to be converted.
 * @return A `Map.Entry` with the first element of the pair as the key and the second element as the value.
 */
private fun <A, B> Pair<A, B>.toMapEntry() = java.util.Map.entry(this.first, this.second)
