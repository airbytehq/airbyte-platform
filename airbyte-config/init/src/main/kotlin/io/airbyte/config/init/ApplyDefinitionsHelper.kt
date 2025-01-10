/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.Configs.SeedDefinitionsProviderType
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.helpers.ConnectorRegistryConverters
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingFailureReason
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingOutcome
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.DefinitionProcessingSuccessOutcome
import io.airbyte.config.init.ApplyDefinitionMetricsHelper.getMetricAttributes
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.config.specs.DefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorRolloutService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.context.annotation.Requires
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.Optional
import java.util.UUID
import kotlin.jvm.optionals.getOrNull

/**
 * Helper class used to apply actor definitions from a DefinitionsProvider to the database. This is
 * here to enable easy reuse of definition application logic in bootloader and cron.
 */
@Singleton
@Requires(bean = JobPersistence::class)
@Requires(bean = MetricClient::class)
class ApplyDefinitionsHelper(
  @param:Named("seedDefinitionsProvider") private val definitionsProvider: DefinitionsProvider,
  private val seedProviderType: SeedDefinitionsProviderType,
  private val jobPersistence: JobPersistence,
  private val actorDefinitionService: ActorDefinitionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val metricClient: MetricClient,
  private val supportStateUpdater: SupportStateUpdater,
  private val actorDefinitionVersionResolver: ActorDefinitionVersionResolver,
  private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
  private val connectorRolloutService: ConnectorRolloutService,
) {
  private var newConnectorCount = 0
  private var changedConnectorCount = 0

  /**
   * Apply the latest definitions from the provider to the repository.
   *
   * @param updateAll - Whether we should overwrite all stored definitions. If true, we do not
   * consider whether a definition is in use before updating the definition and default
   * version.
   * @param reImportVersionInUse - It forces the connector in use to re-import their connector definition.
   */
  @JvmOverloads
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun apply(
    updateAll: Boolean = false,
    reImportVersionInUse: Boolean = false,
  ) {
    val latestSourceDefinitions = definitionsProvider.sourceDefinitions
    val latestDestinationDefinitions = definitionsProvider.destinationDefinitions

    val currentProtocolRange = jobPersistence.currentProtocolVersionRange
    val protocolCompatibleSourceDefinitions =
      filterOutIncompatibleSourceDefs(currentProtocolRange, latestSourceDefinitions)
    val protocolCompatibleDestinationDefinitions =
      filterOutIncompatibleDestDefs(currentProtocolRange, latestDestinationDefinitions)

    val airbyteCompatibleSourceDefinitions =
      filterOutIncompatibleSourceDefsWithCurrentAirbyteVersion(protocolCompatibleSourceDefinitions)
    val airbyteCompatibleDestinationDefinitions =
      filterOutIncompatibleDestinationDefsWithCurrentAirbyteVersion(protocolCompatibleDestinationDefinitions)
    val actorDefinitionIdsToDefaultVersionsMap =
      actorDefinitionService.actorDefinitionIdsToDefaultVersionsMap
    val actorDefinitionIdsInUse = actorDefinitionService.actorDefinitionIdsInUse

    newConnectorCount = 0
    changedConnectorCount = 0
    for (def in airbyteCompatibleSourceDefinitions) {
      applySourceDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll, reImportVersionInUse)
    }
    for (def in airbyteCompatibleDestinationDefinitions) {
      applyDestinationDefinition(actorDefinitionIdsToDefaultVersionsMap, def, actorDefinitionIdsInUse, updateAll, reImportVersionInUse)
    }
    supportStateUpdater.updateSupportStates()
    log.info("New connectors added: {}", newConnectorCount)
    log.info("Version changes applied: {}", changedConnectorCount)
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  private fun applySourceDefinition(
    actorDefinitionIdsAndDefaultVersions: Map<UUID, ActorDefinitionVersion>,
    newDef: ConnectorRegistrySourceDefinition,
    actorDefinitionIdsInUse: Set<UUID>,
    updateAll: Boolean,
    reImportVersionInUse: Boolean,
  ) {
    // Skip and log if unable to parse registry entry.
    val newSourceDef: StandardSourceDefinition
    val newADV: ActorDefinitionVersion
    val rcDefinitions: List<ConnectorRegistrySourceDefinition>
    val breakingChangesForDef: List<ActorDefinitionBreakingChange>
    try {
      newSourceDef = ConnectorRegistryConverters.toStandardSourceDefinition(newDef)
      newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef)

      rcDefinitions =
        run {
          try {
            ConnectorRegistryConverters.toRcSourceDefinitions(newDef)
          } catch (e: Exception) {
            log.error("Could not extract release candidates from the connector definition: {}", newDef.name, e)
            emptyList()
          }
        }

      breakingChangesForDef = ConnectorRegistryConverters.toActorDefinitionBreakingChanges(newDef)
    } catch (e: IllegalArgumentException) {
      log.error("Failed to convert source definition: {}", newDef.name, e)
      trackDefinitionProcessed(
        newDef.dockerRepository,
        newDef.dockerImageTag,
        DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED,
      )
      return
    }

    val connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newSourceDef.sourceDefinitionId)
    if (connectorIsNew) {
      log.info("Adding new connector {}:{}", newDef.dockerRepository, newDef.dockerImageTag)
      sourceService.writeConnectorMetadata(newSourceDef, newADV, breakingChangesForDef)
      newConnectorCount++
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED)
      return
    }

    val currentDefaultADV =
      actorDefinitionIdsAndDefaultVersions[newSourceDef.sourceDefinitionId] ?: throw RuntimeException(
        "No default actor definition version found for source definition with ID: ${newSourceDef.sourceDefinitionId}",
      )
    val shouldUpdateActorDefinitionDefaultVersion =
      getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll)

    val shouldUpdateOldVersionMetadata = (newADV.dockerImageTag != currentDefaultADV.dockerImageTag) && reImportVersionInUse

    if (shouldUpdateActorDefinitionDefaultVersion) {
      log.info(
        "Updating default version for connector {}: {} -> {}",
        currentDefaultADV.dockerRepository,
        currentDefaultADV.dockerImageTag,
        newADV.dockerImageTag,
      )
      sourceService.writeConnectorMetadata(newSourceDef, newADV, breakingChangesForDef)
      changedConnectorCount++
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED)
    } else if (shouldUpdateOldVersionMetadata) {
      log.info("Refreshing default version metadata for connector ${currentDefaultADV.dockerRepository}:${currentDefaultADV.dockerImageTag}")

      val updatedADV =
        actorDefinitionVersionResolver.fetchRemoteActorDefinitionVersion(
          ActorType.SOURCE,
          currentDefaultADV.dockerRepository,
          currentDefaultADV.dockerImageTag,
        )

      updatedADV.ifPresent {
        sourceService.writeConnectorMetadata(newSourceDef, it, breakingChangesForDef)
        changedConnectorCount++
        trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.REFRESH_VERSION)
      }
    } else {
      sourceService.updateStandardSourceDefinition(newSourceDef)
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED)
    }

    applyReleaseCandidates(rcDefinitions)
  }

  @VisibleForTesting
  internal fun <T> applyReleaseCandidates(rcDefinitions: List<T>) {
    for (rcDef in rcDefinitions) {
      val rcAdv =
        when (rcDef) {
          is ConnectorRegistrySourceDefinition -> ConnectorRegistryConverters.toActorDefinitionVersion(rcDef)
          is ConnectorRegistryDestinationDefinition -> ConnectorRegistryConverters.toActorDefinitionVersion(rcDef)
          else -> {
            val rcClass = rcDef!!::class.java
            throw IllegalArgumentException("Unsupported type: $rcClass")
          }
        }

      val insertedAdv = actorDefinitionService.writeActorDefinitionVersion(rcAdv)
      log.info("Inserted or updated release candidate actor definition version for {}:{}", insertedAdv.dockerRepository, insertedAdv.dockerImageTag)
      val initialAdv = actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(rcAdv.actorDefinitionId)
      if (initialAdv.isEmpty) {
        log.error("Could not find default version for actor definition ID: {}", rcAdv.actorDefinitionId)
        continue
      }
      try {
        val connectorRollout =
          when (rcDef) {
            is ConnectorRegistrySourceDefinition -> ConnectorRegistryConverters.toConnectorRollout(rcDef, insertedAdv, initialAdv.getOrNull())
            is ConnectorRegistryDestinationDefinition -> ConnectorRegistryConverters.toConnectorRollout(rcDef, insertedAdv, initialAdv.getOrNull())
            else -> throw IllegalArgumentException("Unsupported type: ${rcDef!!::class.java}")
          }
        val existingRollout =
          connectorRolloutService.listConnectorRollouts(
            connectorRollout.actorDefinitionId,
            connectorRollout.releaseCandidateVersionId,
          )
        if (existingRollout.isNotEmpty() && existingRollout.any { it.state != ConnectorEnumRolloutState.CANCELED }) {
          log.info("Release candidate rollout configuration already exists for {}:{}", insertedAdv.dockerRepository, insertedAdv.dockerImageTag)
          continue
        }
        connectorRolloutService.insertConnectorRollout(connectorRollout)
        log.info(
          "Inserted release candidate rollout configuration for {}:{}; rcActorDefinitionVersion={} defaultActorDefinitionVersion={}",
          insertedAdv.dockerRepository,
          insertedAdv.dockerImageTag,
          insertedAdv.versionId,
          initialAdv.getOrNull()?.versionId,
        )
      } catch (e: Exception) {
        log.error("An error occurred on connector rollout object creation", e)
      }
    }
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  private fun applyDestinationDefinition(
    actorDefinitionIdsAndDefaultVersions: Map<UUID, ActorDefinitionVersion>,
    newDef: ConnectorRegistryDestinationDefinition,
    actorDefinitionIdsInUse: Set<UUID>,
    updateAll: Boolean,
    reImportVersionInUse: Boolean,
  ) {
    // Skip and log if unable to parse registry entry.
    val newDestinationDef: StandardDestinationDefinition
    val newADV: ActorDefinitionVersion
    val rcDefinitions: List<ConnectorRegistryDestinationDefinition>
    val breakingChangesForDef: List<ActorDefinitionBreakingChange>
    try {
      newDestinationDef = ConnectorRegistryConverters.toStandardDestinationDefinition(newDef)
      newADV = ConnectorRegistryConverters.toActorDefinitionVersion(newDef)

      rcDefinitions =
        run {
          try {
            ConnectorRegistryConverters.toRcDestinationDefinitions(newDef)
          } catch (e: Exception) {
            log.error("Could not extract release candidates from the connector definition: {}", newDef.name, e)
            emptyList()
          }
        }

      breakingChangesForDef = ConnectorRegistryConverters.toActorDefinitionBreakingChanges(newDef)
    } catch (e: IllegalArgumentException) {
      log.error("Failed to convert source definition: {}", newDef.name, e)
      trackDefinitionProcessed(
        newDef.dockerRepository,
        newDef.dockerImageTag,
        DefinitionProcessingFailureReason.DEFINITION_CONVERSION_FAILED,
      )
      return
    }

    val connectorIsNew = !actorDefinitionIdsAndDefaultVersions.containsKey(newDestinationDef.destinationDefinitionId)
    if (connectorIsNew) {
      log.info("Adding new connector {}:{}", newDef.dockerRepository, newDef.dockerImageTag)
      destinationService.writeConnectorMetadata(newDestinationDef, newADV, breakingChangesForDef)
      newConnectorCount++
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.INITIAL_VERSION_ADDED)
      return
    }

    val currentDefaultADV =
      actorDefinitionIdsAndDefaultVersions[newDestinationDef.destinationDefinitionId] ?: throw RuntimeException(
        "No default actor definition version found for destination definition with ID: ${newDestinationDef.destinationDefinitionId}",
      )
    val shouldUpdateActorDefinitionDefaultVersion =
      getShouldUpdateActorDefinitionDefaultVersion(currentDefaultADV, newADV, actorDefinitionIdsInUse, updateAll)

    val shouldUpdateOldVersion = getShouldRefreshActorDefinitionDefaultVersion(currentDefaultADV, actorDefinitionIdsInUse, reImportVersionInUse)

    if (shouldUpdateActorDefinitionDefaultVersion) {
      log.info(
        "Updating default version for connector {}: {} -> {}",
        currentDefaultADV.dockerRepository,
        currentDefaultADV.dockerImageTag,
        newADV.dockerImageTag,
      )
      destinationService.writeConnectorMetadata(newDestinationDef, newADV, breakingChangesForDef)
      changedConnectorCount++
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED)
    } else if (shouldUpdateOldVersion) {
      log.info("Refreshing default version metadata for connector ${currentDefaultADV.dockerRepository}:${currentDefaultADV.dockerImageTag}")

      val updatedADV =
        actorDefinitionVersionResolver.fetchRemoteActorDefinitionVersion(
          ActorType.DESTINATION,
          currentDefaultADV.dockerRepository,
          currentDefaultADV.dockerImageTag,
        )

      updatedADV.ifPresent {
        destinationService.writeConnectorMetadata(newDestinationDef, it, breakingChangesForDef)
        changedConnectorCount++
        trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.DEFAULT_VERSION_UPDATED)
      }
    } else {
      destinationService.updateStandardDestinationDefinition(newDestinationDef)
      trackDefinitionProcessed(newDef.dockerRepository, newDef.dockerImageTag, DefinitionProcessingSuccessOutcome.VERSION_UNCHANGED)
    }

    applyReleaseCandidates(rcDefinitions)
  }

  private fun getShouldRefreshActorDefinitionDefaultVersion(
    currentDefaultADV: ActorDefinitionVersion,
    actorDefinitionIdsInUse: Set<UUID>,
    reImportVersionInUse: Boolean,
  ): Boolean {
    val definitionIsInUse = actorDefinitionIdsInUse.contains(currentDefaultADV.actorDefinitionId)

    return reImportVersionInUse && definitionIsInUse
  }

  @VisibleForTesting
  internal fun getShouldUpdateActorDefinitionDefaultVersion(
    currentDefaultADV: ActorDefinitionVersion,
    newADV: ActorDefinitionVersion,
    actorDefinitionIdsInUse: Set<UUID>,
    updateAll: Boolean,
  ): Boolean {
    val newVersionIsAvailable =
      when (seedProviderType) {
        SeedDefinitionsProviderType.REMOTE -> newADV.dockerImageTag != currentDefaultADV.dockerImageTag
        SeedDefinitionsProviderType.LOCAL -> {
          // (oss) if we're using the registry shipped with the platform, connector versions may be stale.
          // We should only update if the new version is greater than the current version, in case the user has manually
          // upgraded the connector via the UI. See https://github.com/airbytehq/airbyte-internal-issues/issues/8691.
          newADV.dockerImageTag > currentDefaultADV.dockerImageTag
        }
      }

    val definitionIsInUse = actorDefinitionIdsInUse.contains(currentDefaultADV.actorDefinitionId)
    val shouldApplyNewVersion = updateAll || !definitionIsInUse

    return newVersionIsAvailable && shouldApplyNewVersion
  }

  private fun filterOutIncompatibleDestDefs(
    protocolVersionRange: Optional<AirbyteProtocolVersionRange>,
    destDefs: List<ConnectorRegistryDestinationDefinition>,
  ): List<ConnectorRegistryDestinationDefinition> {
    if (protocolVersionRange.isEmpty) {
      return destDefs
    }

    return destDefs.stream().filter { def: ConnectorRegistryDestinationDefinition ->
      val isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.spec.protocolVersion)
      if (!isSupported) {
        log.warn(
          "Destination {} {} has an incompatible protocol version ({})... ignoring.",
          def.destinationDefinitionId,
          def.name,
          def.spec.protocolVersion,
        )
        trackDefinitionProcessed(def.dockerRepository, def.dockerImageTag, DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION)
      }
      isSupported
    }.toList()
  }

  private fun filterOutIncompatibleSourceDefs(
    protocolVersionRange: Optional<AirbyteProtocolVersionRange>,
    sourceDefs: List<ConnectorRegistrySourceDefinition>,
  ): List<ConnectorRegistrySourceDefinition> {
    if (protocolVersionRange.isEmpty) {
      return sourceDefs
    }

    return sourceDefs.stream().filter { def: ConnectorRegistrySourceDefinition ->
      val isSupported = isProtocolVersionSupported(protocolVersionRange.get(), def.spec.protocolVersion)
      if (!isSupported) {
        log.warn(
          "Source {} {} has an incompatible protocol version ({})... ignoring.",
          def.sourceDefinitionId,
          def.name,
          def.spec.protocolVersion,
        )
        trackDefinitionProcessed(def.dockerRepository, def.dockerImageTag, DefinitionProcessingFailureReason.INCOMPATIBLE_PROTOCOL_VERSION)
      }
      isSupported
    }.toList()
  }

  private fun filterOutIncompatibleSourceDefsWithCurrentAirbyteVersion(
    sourceDefs: List<ConnectorRegistrySourceDefinition>,
  ): List<ConnectorRegistrySourceDefinition> {
    return sourceDefs.stream().filter { def: ConnectorRegistrySourceDefinition ->
      val isConnectorSupported = airbyteCompatibleConnectorsValidator.validate(def.sourceDefinitionId.toString(), def.dockerImageTag)
      if (!isConnectorSupported.isValid) {
        log.warn(isConnectorSupported.message)
        trackDefinitionProcessed(def.dockerRepository, def.dockerImageTag, DefinitionProcessingFailureReason.INCOMPATIBLE_AIRBYTE_VERSION)
      }
      isConnectorSupported.isValid
    }.toList()
  }

  private fun filterOutIncompatibleDestinationDefsWithCurrentAirbyteVersion(
    destinationDefs: List<ConnectorRegistryDestinationDefinition>,
  ): List<ConnectorRegistryDestinationDefinition> {
    return destinationDefs.stream().filter { def: ConnectorRegistryDestinationDefinition ->
      val isNewConnectorVersionSupported = airbyteCompatibleConnectorsValidator.validate(def.destinationDefinitionId.toString(), def.dockerImageTag)
      if (!isNewConnectorVersionSupported.isValid) {
        log.warn(isNewConnectorVersionSupported.message)
        trackDefinitionProcessed(def.dockerRepository, def.dockerImageTag, DefinitionProcessingFailureReason.INCOMPATIBLE_AIRBYTE_VERSION)
      }
      isNewConnectorVersionSupported.isValid
    }.toList()
  }

  private fun isProtocolVersionSupported(
    protocolVersionRange: AirbyteProtocolVersionRange,
    protocolVersion: String?,
  ): Boolean {
    return protocolVersionRange.isSupported(AirbyteProtocolVersion.getWithDefault(protocolVersion))
  }

  private fun trackDefinitionProcessed(
    dockerRepository: String,
    dockerImageTag: String,
    outcome: DefinitionProcessingOutcome,
  ) {
    val attributes = getMetricAttributes(dockerRepository, dockerImageTag, outcome)
    metricClient.count(OssMetricsRegistry.CONNECTOR_REGISTRY_DEFINITION_PROCESSED, 1, *attributes)
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(ApplyDefinitionsHelper::class.java)
  }
}
