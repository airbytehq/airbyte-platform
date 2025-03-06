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
import io.airbyte.protocol.models.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito
import java.util.Optional
import java.util.UUID

internal class ProtocolVersionCheckerTest {
  var definitionsProvider: DefinitionsProvider? = null
  var jobPersistence: JobPersistence? = null
  var actorDefinitionService: ActorDefinitionService? = null
  var sourceService: SourceService? = null
  var destinationService: DestinationService? = null

  @BeforeEach
  fun beforeEach() {
    actorDefinitionService = Mockito.mock(ActorDefinitionService::class.java)
    definitionsProvider = Mockito.mock(DefinitionsProvider::class.java)
    jobPersistence = Mockito.mock(JobPersistence::class.java)
    sourceService = Mockito.mock(SourceService::class.java)
    destinationService = Mockito.mock(DestinationService::class.java)

    Mockito.`when`(jobPersistence?.getVersion()).thenReturn(Optional.of("1.2.3"))
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testFirstInstallCheck(supportAutoUpgrade: Boolean) {
    val expectedRange = AirbyteProtocolVersionRange(V0_0_0, V1_0_0)
    Mockito.`when`(jobPersistence!!.version).thenReturn(Optional.empty())
    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        expectedRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
        jobPersistence!!,
        expectedRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.Map.of(
        source1,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        source2,
        java.util.Map.entry(ActorType.SOURCE, V1_0_0),
        source3,
        java.util.Map.entry(ActorType.SOURCE, V2_0_0),
        dest1,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
        dest2,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
      )
    Mockito
      .`when`(
        actorDefinitionService!!.actorDefinitionToProtocolVersionMap,
      ).thenReturn(initialActorDefinitions)

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        targetRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
      )
    val conflicts: Map<ActorType, Set<UUID>> = protocolVersionChecker.getConflictingActorDefinitions(targetRange)

    val expectedConflicts =
      java.util.Map.of(
        ActorType.DESTINATION,
        java.util.Set.of(dest1, dest2),
        ActorType.SOURCE,
        java.util.Set.of(source1),
      )
    Assertions.assertEquals(expectedConflicts, conflicts)
  }

  @Test
  fun testRetrievingCurrentConflictsWhenNoConflicts() {
    val targetRange = AirbyteProtocolVersionRange(V1_0_0, V2_0_0)

    val source1 = UUID.randomUUID()
    val dest1 = UUID.randomUUID()

    val initialActorDefinitions =
      java.util.Map.of(
        source1,
        java.util.Map.entry(ActorType.SOURCE, V2_0_0),
        dest1,
        java.util.Map.entry(ActorType.DESTINATION, V1_0_0),
      )
    Mockito
      .`when`(
        actorDefinitionService!!.actorDefinitionToProtocolVersionMap,
      ).thenReturn(initialActorDefinitions)

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        targetRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.List.of(
        java.util.Map.entry(unrelatedSource, V2_0_0),
        java.util.Map.entry(upgradedSource, V1_0_0),
        java.util.Map.entry(notChangedSource, V0_0_0),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        targetRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.List.of(
        java.util.Map.entry(dest1, V2_0_0),
        java.util.Map.entry(dest2, V1_0_0),
        java.util.Map.entry(dest3, V2_0_0),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        targetRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
        jobPersistence!!,
        expectedRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.Map.of(
        source1,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        source2,
        java.util.Map.entry(ActorType.SOURCE, V1_0_0),
        source3,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        source4,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        dest1,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
        dest2,
        java.util.Map.entry(ActorType.DESTINATION, V1_0_0),
        dest3,
        java.util.Map.entry(ActorType.DESTINATION, V2_0_0),
      )
    Mockito
      .`when`(
        actorDefinitionService!!.actorDefinitionToProtocolVersionMap,
      ).thenReturn(initialActorDefinitions)

    setNewSourceDefinitions(
      java.util.List.of(
        java.util.Map.entry(source1, V1_0_0),
        java.util.Map.entry(source2, V1_0_0),
        java.util.Map.entry(source3, V2_0_0),
        java.util.Map.entry(source4, V1_0_0),
      ),
    )
    setNewDestinationDefinitions(
      java.util.List.of(
        java.util.Map.entry(dest1, V1_0_0),
        java.util.Map.entry(dest2, V1_0_0),
        java.util.Map.entry(dest3, V2_0_0),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        expectedTargetVersionRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.Map.of(
        source1,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        source2,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        dest1,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
        dest2,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
      )
    Mockito
      .`when`(
        actorDefinitionService!!.actorDefinitionToProtocolVersionMap,
      ).thenReturn(initialActorDefinitions)

    setNewSourceDefinitions(
      java.util.List.of(
        java.util.Map.entry(source1, V0_0_0),
        java.util.Map.entry(source2, V1_0_0),
      ),
    )
    setNewDestinationDefinitions(
      java.util.List.of(
        java.util.Map.entry(dest1, V1_0_0),
        java.util.Map.entry(dest2, V1_0_0),
      ),
    )

    val protocolVersionChecker =
      ProtocolVersionChecker(
        jobPersistence!!,
        expectedTargetVersionRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
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
      java.util.Map.of(
        source1,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        source2,
        java.util.Map.entry(ActorType.SOURCE, V0_0_0),
        dest1,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
        dest2,
        java.util.Map.entry(ActorType.DESTINATION, V0_0_0),
      )
    Mockito
      .`when`(
        actorDefinitionService!!.actorDefinitionToProtocolVersionMap,
      ).thenReturn(initialActorDefinitions)

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
        jobPersistence!!,
        expectedTargetVersionRange,
        actorDefinitionService!!,
        definitionsProvider!!,
        sourceService!!,
        destinationService!!,
      )
    val actualRange = protocolVersionChecker.validate(supportAutoUpgrade)
    Assertions.assertNull(actualRange)
  }

  private fun setCurrentProtocolRangeRange(
    min: Version,
    max: Version,
  ) {
    Mockito.`when`(jobPersistence!!.currentProtocolVersionRange).thenReturn(Optional.of(AirbyteProtocolVersionRange(min, max)))
    Mockito.`when`(jobPersistence!!.airbyteProtocolVersionMin).thenReturn(Optional.of(min))
    Mockito.`when`(jobPersistence!!.airbyteProtocolVersionMax).thenReturn(Optional.of(max))
  }

  private fun setNewDestinationDefinitions(defs: List<Map.Entry<UUID, Version>>) {
    val destDefinitions =
      defs
        .stream()
        .map { e: Map.Entry<UUID, Version> ->
          ConnectorRegistryDestinationDefinition()
            .withDestinationDefinitionId(e.key)
            .withSpec(ConnectorSpecification().withProtocolVersion(e.value.serialize()))
        }.toList()
    Mockito.`when`(definitionsProvider!!.destinationDefinitions).thenReturn(destDefinitions)
  }

  private fun setNewSourceDefinitions(defs: List<Map.Entry<UUID, Version>>) {
    val sourceDefinitions =
      defs
        .stream()
        .map { e: Map.Entry<UUID, Version> ->
          ConnectorRegistrySourceDefinition()
            .withSourceDefinitionId(e.key)
            .withSpec(ConnectorSpecification().withProtocolVersion(e.value.serialize()))
        }.toList()
    Mockito.`when`(definitionsProvider!!.sourceDefinitions).thenReturn(sourceDefinitions)
  }

  companion object {
    private val V0_0_0 = Version("0.0.0")
    private val V1_0_0 = Version("1.0.0")
    private val V2_0_0 = Version("2.0.0")
  }
}
