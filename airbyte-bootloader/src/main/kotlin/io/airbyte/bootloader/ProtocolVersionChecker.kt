/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.bootloader

import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.commons.version.Version
import io.airbyte.config.ActorType
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.persistence.job.JobPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Validates that all connectors support the desired target Airbyte protocol version.
 *
 * Constructor creates a new protocol version checker that verifies all connectors are within the provided
 * target protocol version range.
 *
 * @param jobPersistence A [JobPersistence] instance.
 * @param airbyteProtocolTargetVersionRange The target Airbyte protocol version range.
 * @param actorDefinitionService The service for actor definitions [ActorDefinitionService]
 * @param definitionsProvider The [DefinitionsProvider] used for seeding.
 * @param sourceService The service for sources [SourceService]
 * @param destinationService The Service for destinations [DestinationService]
 */
@Singleton
class ProtocolVersionChecker(
  private val jobPersistence: JobPersistence,
  val targetProtocolVersionRange: AirbyteProtocolVersionRange,
  private val actorDefinitionService: ActorDefinitionService,
  @param:Named("seedDefinitionsProvider") private val definitionsProvider: DefinitionsProvider,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
) {
  /**
   * Validate the AirbyteProtocolVersion support range between the platform and the connectors.
   *
   *
   * The goal is to make sure that we do not end up disabling existing connections after an upgrade
   * that changes the protocol support range.
   *
   * @param supportAutoUpgrade whether the connectors will be automatically upgraded by the platform
   * @return the supported protocol version range if check is successful, Optional.empty() if we would
   * break existing connections.
   * @throws IOException when interacting with the db.
   */
  fun validate(supportAutoUpgrade: Boolean): AirbyteProtocolVersionRange? {
    val currentAirbyteVersion = currentAirbyteVersion
    val currentRange = jobPersistence.getCurrentProtocolVersionRange()
    val targetRange = targetProtocolVersionRange

    // Checking if there is a pre-existing version of airbyte.
    // Without this check, the first run of the validation would fail because we do not have the tables
    // set yet
    // which means that the actor definitions lookup will throw SQLExceptions.
    if (currentAirbyteVersion.isEmpty) {
      log.info { "No previous version of Airbyte detected, assuming this is a fresh deploy." }
      return targetRange
    }

    if (currentRange.isEmpty || currentRange.get() == targetRange) {
      log.info { "Using AirbyteProtocolVersion range [${targetRange.min.serialize()}:${targetRange.max.serialize()}]" }
      return targetRange
    }

    log.info {
      "${"Detected an AirbyteProtocolVersion range change from [{}:{}] to [{}:{}]"} ${
        arrayOf<Any?>(
          currentRange.get().min.serialize(),
          currentRange.get().max.serialize(),
          targetRange.min.serialize(),
          targetRange.max.serialize(),
        )
      }"
    }

    val conflicts = getConflictingActorDefinitions(targetRange)

    if (conflicts.isEmpty()) {
      log.info { "No protocol version conflict detected." }
      return targetRange
    }

    val destConflicts = conflicts.getOrDefault(ActorType.DESTINATION, HashSet())
    val sourceConflicts = conflicts.getOrDefault(ActorType.SOURCE, HashSet())

    if (!supportAutoUpgrade) {
      // If we do not support auto upgrade, any conflict of used connectors must be resolved before being
      // able to upgrade the platform.
      log.warn { "The following connectors need to be upgraded before being able to upgrade the platform" }
      formatActorDefinitionForLogging(destConflicts, sourceConflicts).forEach { msg: String? -> log.warn { msg } }
      return null
    }

    val remainingDestConflicts =
      projectRemainingConflictsAfterConnectorUpgrades(targetRange, destConflicts, ActorType.DESTINATION)
    val remainingSourceConflicts =
      projectRemainingConflictsAfterConnectorUpgrades(targetRange, sourceConflicts, ActorType.SOURCE)

    if (!remainingDestConflicts.isEmpty() || !remainingSourceConflicts.isEmpty()) {
      // These set of connectors need a manual intervention because there is no compatible version listed
      formatActorDefinitionForLogging(remainingDestConflicts, remainingSourceConflicts).forEach { msg: String? -> log.warn { msg } }
      return null
    }

    // These can be auto upgraded
    destConflicts.removeAll(remainingDestConflicts)
    sourceConflicts.removeAll(remainingSourceConflicts)
    log.info { "The following connectors will be upgraded" }
    formatActorDefinitionForLogging(destConflicts, sourceConflicts).forEach { msg: String? -> log.info { msg } }
    return targetRange
  }

  @get:Throws(IOException::class)
  protected val currentAirbyteVersion: Optional<AirbyteVersion>
    get() = jobPersistence.getVersion().map { version: String -> AirbyteVersion(version) }

  fun getConflictingActorDefinitions(targetRange: AirbyteProtocolVersionRange): Map<ActorType, MutableSet<UUID>> {
    val actorDefIdToProtocolVersion = actorDefinitionService.getActorDefinitionToProtocolVersionMap()
    val conflicts: Map<ActorType, MutableSet<UUID>> =
      actorDefIdToProtocolVersion
        .filter { !targetRange.isSupported(it.value.value) } // Keep unsupported protocol versions
        .map { it.value.key to it.key } // Map to Pair<ActorType, UUID>
        .groupBy({ it.first }, { it.second }) // Group by ActorType, collect UUIDs
        .mapValues { it.value.toMutableSet() } // Convert List<UUID> to Set<UUID>
    return conflicts
  }

  fun projectRemainingConflictsAfterConnectorUpgrades(
    targetRange: AirbyteProtocolVersionRange,
    initialConflicts: Set<UUID>,
    actorType: ActorType,
  ): Set<UUID> {
    if (initialConflicts.isEmpty()) {
      return setOf()
    }

    val upgradedSourceDefs =
      getProtocolVersionsForActorDefinitions(actorType) // Keep definition ids if the protocol version will fall into the new supported range
        .filter { e: Map.Entry<UUID, Version> -> initialConflicts.contains(e.key) && targetRange.isSupported(e.value) }
        .map { obj: Map.Entry<UUID, Version> -> obj.key }
        .toSet()

    // Get the set of source definitions that will still have conflict after the connector upgrades
    val remainingConflicts: MutableSet<UUID> = HashSet(initialConflicts)
    remainingConflicts.removeAll(upgradedSourceDefs)
    return remainingConflicts
  }

  protected fun getProtocolVersionsForActorDefinitions(actorType: ActorType): Sequence<Map.Entry<UUID, Version>> = getActorVersions(actorType)

  private fun getActorVersions(actorType: ActorType): Sequence<Map.Entry<UUID, Version>> =
    when (actorType) {
      ActorType.SOURCE ->
        definitionsProvider
          .getSourceDefinitions()
          .asSequence()
          .map { def ->
            def.sourceDefinitionId to AirbyteProtocolVersion.getWithDefault(def.spec.protocolVersion)
          }

      ActorType.DESTINATION ->
        definitionsProvider
          .getDestinationDefinitions()
          .asSequence()
          .map { def ->
            def.destinationDefinitionId to AirbyteProtocolVersion.getWithDefault(def.spec.protocolVersion)
          }

      else ->
        definitionsProvider
          .getDestinationDefinitions()
          .asSequence()
          .map { def ->
            def.destinationDefinitionId to AirbyteProtocolVersion.getWithDefault(def.spec.protocolVersion)
          }
    }.map { (id, version) -> java.util.AbstractMap.SimpleEntry(id, version) }

  private fun formatActorDefinitionForLogging(
    remainingDestConflicts: Set<UUID>,
    remainingSourceConflicts: Set<UUID>,
  ): Sequence<String> =
    sequence {
      remainingSourceConflicts.forEach { defId ->
        yield(
          try {
            val sourceDef = sourceService.getStandardSourceDefinition(defId)
            val sourceDefVersion = actorDefinitionService.getActorDefinitionVersion(sourceDef.defaultVersionId)
            "Source: ${sourceDef.sourceDefinitionId}: ${sourceDef.name}: protocol version: ${sourceDefVersion.protocolVersion}"
          } catch (e: Exception) {
            log.info { "Failed to getStandardSourceDefinition for $defId $e" }
            "Source: $defId: Failed to fetch details..."
          },
        )
      }

      remainingDestConflicts.forEach { defId ->
        yield(
          try {
            val destDef = destinationService.getStandardDestinationDefinition(defId)
            val destDefVersion = actorDefinitionService.getActorDefinitionVersion(destDef.defaultVersionId)
            "Destination: ${destDef.destinationDefinitionId}: ${destDef.name}: protocol version: ${destDefVersion.protocolVersion}"
          } catch (e: Exception) {
            log.info { "Failed to getStandardDestinationDefinition for $defId $e" }
            "Destination: $defId: Failed to fetch details..."
          },
        )
      }
    }
}
