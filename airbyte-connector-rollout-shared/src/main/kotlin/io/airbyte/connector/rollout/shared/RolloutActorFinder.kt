package io.airbyte.connector.rollout.shared

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.Schedule
import io.airbyte.config.StandardSync
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.CustomerTier
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.math.ceil

private val logger = KotlinLogging.logger {}

data class ActorSelectionInfo(
  val actorIdsToPin: List<UUID>,
  val nActors: Int,
  val nActorsEligibleOrAlreadyPinned: Int,
  val nNewPinned: Int = actorIdsToPin.size,
  val nPreviouslyPinned: Int,
)

data class ActorSyncJobInfo(
  var nSucceeded: Int = 0,
  var nFailed: Int = 0,
  var nConnections: Int = 0,
)

@Singleton
class RolloutActorFinder(
  private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
  private val connectionService: ConnectionService,
  private val jobService: JobService,
  private val scopedConfigurationService: ScopedConfigurationService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val organizationCustomerAttributesService: OrganizationCustomerAttributesService,
) {
  fun getActorSelectionInfo(
    connectorRollout: ConnectorRollout,
    targetPercent: Int?,
  ): ActorSelectionInfo {
    logger.info { "Finding actors to pin for rollout ${connectorRollout.id} targetPercent=$targetPercent" }

    var candidates = actorDefinitionVersionUpdater.getConfigScopeMaps(connectorRollout.actorDefinitionId)
    val initialNCandidates = candidates.size

    logger.info { "Rollout ${connectorRollout.id}: $initialNCandidates actors total" }

    val actorType = getActorType(connectorRollout.actorDefinitionId)
    candidates = filterByTier(candidates)

    logger.info { "Rollout ${connectorRollout.id}: ${candidates.size} after filterByTier" }

    val sortedActorDefinitionConnections =
      getSortedActorDefinitionConnections(candidates.map { it.id }, connectorRollout.actorDefinitionId, actorType)
    candidates = filterByJobStatus(connectorRollout, candidates, sortedActorDefinitionConnections, actorType)

    logger.info {
      "Rollout ${connectorRollout.id}: " +
        "${sortedActorDefinitionConnections.size} connections total;  ${candidates.size} after filterByJobStatus"
    }

    val nPreviouslyPinned = getActorsPinnedToReleaseCandidate(connectorRollout).size
    logger.info { "Rollout ${connectorRollout.id}: $nPreviouslyPinned already pinned to the release candidate" }

    candidates = filterByAlreadyPinned(connectorRollout.actorDefinitionId, candidates)
    val nEligibleOrAlreadyPinned = candidates.size + nPreviouslyPinned

    logger.info { "Rollout ${connectorRollout.id}: $nEligibleOrAlreadyPinned including eligible & already pinned to the release candidate" }
    logger.info { "Rollout ${connectorRollout.id}: ${nEligibleOrAlreadyPinned - candidates.size - nPreviouslyPinned} pinned to a non-RC" }

    if (targetPercent == null || targetPercent == 0) {
      return ActorSelectionInfo(
        actorIdsToPin = emptyList(),
        nActors = initialNCandidates,
        nActorsEligibleOrAlreadyPinned = nEligibleOrAlreadyPinned,
        nPreviouslyPinned = nPreviouslyPinned,
      )
    }

    // Calculate the number to pin based on the input percentage
    val targetTotalToPin = getTargetTotalToPin(nEligibleOrAlreadyPinned, nPreviouslyPinned, targetPercent)
    val filteredActorDefinitionConnections = filterByConnectionActorId(candidates, sortedActorDefinitionConnections, actorType)
    logger.info {
      "Rollout ${connectorRollout.id}: " +
        "candidates.size=$candidates.size " +
        "sortedActorDefinitionConnections.size=${sortedActorDefinitionConnections.size} " +
        "filteredActorDefinitionConnections.size=${filteredActorDefinitionConnections.size}"
    }

    // From the eligible actors, choose the ones with the next sync
    // TODO: filter out those with lots of data
    // TODO: prioritize internal connections
    val actorIdsToPin =
      getUniqueActorIds(
        filteredActorDefinitionConnections,
        targetTotalToPin,
        actorType,
      )

    logger.info { "Rollout ${connectorRollout.id}: targetTotalToPin=$targetTotalToPin; pinning ${actorIdsToPin.size} actors" }

    return ActorSelectionInfo(
      actorIdsToPin = actorIdsToPin,
      nActors = initialNCandidates,
      nActorsEligibleOrAlreadyPinned = nEligibleOrAlreadyPinned,
      nNewPinned = actorIdsToPin.size,
      nPreviouslyPinned = nPreviouslyPinned,
    )
  }

  @VisibleForTesting
  internal fun getTargetTotalToPin(
    nEligibleOrAlreadyPinned: Int,
    nPreviouslyPinned: Int,
    targetPercentage: Int,
  ): Int {
    logger.info {
      "getTargetTotalToPin nEligibleOrAlreadyPinned=$nEligibleOrAlreadyPinned nPreviouslyPinned=$nPreviouslyPinned targetPercentage=$targetPercentage"
    }
    if (nEligibleOrAlreadyPinned == 0 || targetPercentage <= 0) {
      return 0
    }

    val targetTotal = ceil(nEligibleOrAlreadyPinned.toDouble() * targetPercentage / 100).toInt()
    logger.info { "getTargetTotalToPin targetTotal=$targetTotal" }

    if (nPreviouslyPinned >= targetTotal) {
      return 0
    }

    val actorsToPin = targetTotal - nPreviouslyPinned

    return minOf(actorsToPin, nEligibleOrAlreadyPinned - nPreviouslyPinned)
  }

  @VisibleForTesting
  internal fun filterByConnectionActorId(
    candidates: Collection<ConfigScopeMapWithId>,
    sortedActorDefinitionConnections: List<StandardSync>,
    actorType: ActorType,
  ): List<StandardSync> {
    val candidateIds = candidates.map { it.id }.toSet()

    return sortedActorDefinitionConnections.filter { connection ->
      val relevantId =
        if (actorType == ActorType.SOURCE) {
          connection.sourceId
        } else {
          connection.destinationId
        }
      candidateIds.contains(relevantId)
    }
  }

  fun getSyncInfoForPinnedActors(connectorRollout: ConnectorRollout): Map<UUID, ActorSyncJobInfo> {
    val actorType = getActorType(connectorRollout.actorDefinitionId)

    val pinnedActors = getActorsPinnedToReleaseCandidate(connectorRollout)
    logger.info { "Connector rollout getting sync info for pinned actors: connectorRollout=${connectorRollout.id} pinnedActors=$pinnedActors" }

    val pinnedActorSyncs =
      getSortedActorDefinitionConnectionsByActorId(
        pinnedActors,
        connectorRollout.actorDefinitionId,
        actorType,
      )

    logger.info { "Rollout ${connectorRollout.id}: ${pinnedActorSyncs.size} connections for pinned actors" }

    return getActorJobInfo(
      connectorRollout,
      pinnedActorSyncs,
      actorType,
      Instant.ofEpochMilli(connectorRollout.createdAt).atOffset(ZoneOffset.UTC),
      connectorRollout.releaseCandidateVersionId,
    )
  }

  @VisibleForTesting
  fun getActorJobInfo(
    connectorRollout: ConnectorRollout,
    actorSyncs: List<StandardSync>,
    actorType: ActorType,
    createdAt: OffsetDateTime?,
    versionId: UUID?,
  ): Map<UUID, ActorSyncJobInfo> {
    val actorSyncJobInfoMap = mutableMapOf<UUID, ActorSyncJobInfo>()

    logger.info {
      "Connector rollout getting actorSyncJobInfo connectorRollout.id=${connectorRollout.id} actorSyncs=${actorSyncs.size} versionId=$versionId"
    }

    for (connection in actorSyncs) {
      val actorId = (if (actorType == ActorType.SOURCE) connection.sourceId else connection.destinationId) ?: continue
      val allConnectionJobs =
        jobService.listJobs(
          setOf(JobConfig.ConfigType.SYNC),
          connection.connectionId.toString(),
          1,
          0,
          listOf(),
          createdAt ?: OffsetDateTime.now().minusDays(1),
          null,
          null,
          null,
        )
      logger.info { "connectorRollout.id=${connectorRollout.id} actorId=$actorId allConnectionJobs.size=${allConnectionJobs.size}" }

      val filteredConnectionJobs =
        allConnectionJobs.filter {
          if (versionId != null) {
            jobDefinitionVersionIdEq(actorType, it, versionId)
          } else {
            jobDockerImageIsDefault(actorType, it)
          }
        }
      val nSucceeded = filteredConnectionJobs.filter { it.status == JobStatus.SUCCEEDED }.size
      val nFailed = filteredConnectionJobs.filter { it.status == JobStatus.FAILED }.size
      logger.info {
        "connectorRollout.id=${connectorRollout.id} actorId=$actorId " +
          "filteredConnectionJobs.size=${filteredConnectionJobs.size} nSucceeded=$nSucceeded nFailed=$nFailed"
      }

      if (actorSyncJobInfoMap.containsKey(actorId)) {
        actorSyncJobInfoMap[actorId]!!.nSucceeded += nSucceeded
        actorSyncJobInfoMap[actorId]!!.nFailed += nSucceeded
      } else {
        actorSyncJobInfoMap[actorId] = ActorSyncJobInfo(nSucceeded = nSucceeded, nFailed = nFailed)
      }
      actorSyncJobInfoMap[actorId]!!.nConnections++
    }

    logger.info { "connectorRollout.id=${connectorRollout.id} actorActorSyncJobInfoMap=$actorSyncJobInfoMap" }

    return actorSyncJobInfoMap
  }

  @VisibleForTesting
  fun jobDefinitionVersionIdEq(
    actorType: ActorType,
    job: Job,
    versionId: UUID,
  ): Boolean {
    return if (actorType == ActorType.SOURCE) {
      job.config.sync.sourceDefinitionVersionId == versionId
    } else {
      job.config.sync.destinationDefinitionVersionId == versionId
    }
  }

  @VisibleForTesting
  fun jobDockerImageIsDefault(
    actorType: ActorType,
    job: Job,
  ): Boolean {
    return if (actorType == ActorType.SOURCE) {
      job.config.sync.sourceDockerImageIsDefault
    } else {
      job.config.sync.destinationDockerImageIsDefault
    }
  }

  @VisibleForTesting
  fun getActorType(actorDefinitionId: UUID): ActorType {
    return try {
      sourceService.getStandardSourceDefinition(actorDefinitionId)
      ActorType.SOURCE
    } catch (e: ConfigNotFoundException) {
      try {
        destinationService.getStandardDestinationDefinition(actorDefinitionId)
        ActorType.DESTINATION
      } catch (e: ConfigNotFoundException) {
        throw IllegalStateException(
          "ActorDefinitionId $actorDefinitionId not found by `sourceService.getStandardSourceDefinition` or " +
            "`destinationService.getStandardDestinationDefinition`. This is unexpected.",
        )
      }
    }
  }

  @VisibleForTesting
  fun filterByTier(candidates: Collection<ConfigScopeMapWithId>): Collection<ConfigScopeMapWithId> {
    val organizationTiers = organizationCustomerAttributesService.getOrganizationTiers()
    logger.debug { "RolloutActorFinder.filterByTier: organizationTiers=$organizationTiers" }
    return candidates.filter { candidate ->
      val organizationId = candidate.scopeMap[ConfigScopeType.ORGANIZATION]
      // Include the candidate if the organization ID is not in the map or if the CustomerTier is not TIER_0 or TIER_1
      organizationId == null || organizationTiers[organizationId]?.let { tier ->
        tier != CustomerTier.TIER_0 && tier != CustomerTier.TIER_1
      } ?: true
    }
  }

  @VisibleForTesting
  fun filterByAlreadyPinned(
    actorDefinitionId: UUID,
    configScopeMaps: Collection<ConfigScopeMapWithId>,
  ): Collection<ConfigScopeMapWithId> {
    val eligible =
      actorDefinitionVersionUpdater.getUpgradeCandidates(
        actorDefinitionId,
        configScopeMaps,
      )
    return configScopeMaps.filter {
      eligible.contains(it.id)
    }
  }

  @VisibleForTesting
  fun getActorsPinnedToReleaseCandidate(connectorRollout: ConnectorRollout): List<UUID> {
    val scopedConfigurations =
      scopedConfigurationService.listScopedConfigurationsWithValues(
        ConnectorVersionKey.key,
        ConfigResourceType.ACTOR_DEFINITION,
        connectorRollout.actorDefinitionId,
        ConfigScopeType.ACTOR,
        ConfigOriginType.CONNECTOR_ROLLOUT,
        listOf(connectorRollout.releaseCandidateVersionId.toString()),
      )

    logger.info {
      "Getting actors pinned to release candidate: connectorRollout.id=${connectorRollout.id} scopedConfigurations=$scopedConfigurations"
    }

    val filtered =
      scopedConfigurations.filter {
        it.value == connectorRollout.releaseCandidateVersionId.toString() &&
          it.originType == ConfigOriginType.CONNECTOR_ROLLOUT
      }.map { it.scopeId }

    logger.info { "getActorsPinnedToReleaseCandidate  connectorRollout.id=${connectorRollout.id} filtered=$filtered" }
    return filtered
  }

  @VisibleForTesting
  fun getSortedActorDefinitionConnections(
    actorIds: List<UUID>,
    actorDefinitionId: UUID,
    actorType: ActorType,
  ): List<StandardSync> {
    logger.info {
      "Connector rollout getting sorted actor definition connections: actorIds=$actorIds actorDefinitionId=$actorDefinitionId actorType=$actorType"
    }

    val connections =
      connectionService.listConnectionsByActorDefinitionIdAndType(
        actorDefinitionId,
        actorType.toString(),
        false,
        false,
      )
    logger.info { "getSortedActorDefinitionConnections connections=${connections.size}" }
    for (connection in connections) {
      logger.debug { "getSortedActorDefinitionConnections connection sourceId=${connection.sourceId} destId=${connection.destinationId}" }
    }

    val sortedSyncs =
      connections.filter { connection ->
        when (actorType) {
          ActorType.SOURCE -> connection.sourceId in actorIds
          ActorType.DESTINATION -> connection.destinationId in actorIds
        }
      }.filter { connection ->
        connection.manual != true
      }.sortedBy { connection ->
        getFrequencyInMinutes(connection.schedule)
      }

    logger.info { "Connector rollout sorted actor definition connections: sortedSyncs.size=${sortedSyncs.size}" }
    for (sync in sortedSyncs) {
      logger.debug { "getSortedActorDefinitionConnections sorted sourceId=${sync.sourceId} destId=${sync.destinationId}" }
    }
    return sortedSyncs
  }

  @VisibleForTesting
  fun getSortedActorDefinitionConnectionsByActorId(
    actorIds: List<UUID>,
    actorDefinitionId: UUID,
    actorType: ActorType,
  ): List<StandardSync> {
    logger.info {
      "Connector rollout getting sorted actor definition connections for actor Ids: " +
        "actorIds=$actorIds actorDefinitionId=$actorDefinitionId actorType=$actorType"
    }
    var connections: List<StandardSync>

    if (actorType == ActorType.SOURCE) {
      connections =
        connectionService.listConnectionsBySources(
          actorIds,
          false,
          false,
        )
    } else {
      connections =
        connectionService.listConnectionsByDestinations(
          actorIds,
          false,
          false,
        )
    }
    logger.info { "getSortedActorDefinitionConnectionsByActorId connections=${connections.size}" }
    for (connection in connections) {
      logger.debug { "getSortedActorDefinitionConnectionsByActorId connection sourceId=${connection.sourceId} destId=${connection.destinationId}" }
    }

    val sortedSyncs =
      connections.filter { connection ->
        when (actorType) {
          ActorType.SOURCE -> connection.sourceId in actorIds
          ActorType.DESTINATION -> connection.destinationId in actorIds
        }
      }.sortedBy { connection ->
        getFrequencyInMinutes(connection.schedule)
      }

    logger.info { "Connector rollout sorted actor definition connections: sortedSyncs.size=${sortedSyncs.size}" }
    for (sync in sortedSyncs) {
      logger.debug { "getSortedActorDefinitionConnections sorted sourceId=${sync.sourceId} destId=${sync.destinationId}" }
    }
    return sortedSyncs
  }

  @VisibleForTesting
  fun getFrequencyInMinutes(schedule: Schedule?): Long {
    return if (schedule?.units == null) {
      Long.MAX_VALUE
    } else {
      when (schedule.timeUnit) {
        Schedule.TimeUnit.MINUTES -> schedule.units
        Schedule.TimeUnit.HOURS -> schedule.units * 60
        Schedule.TimeUnit.DAYS -> schedule.units * 60 * 24
        Schedule.TimeUnit.WEEKS -> schedule.units * 60 * 24 * 7
        Schedule.TimeUnit.MONTHS -> schedule.units * 60 * 24 * 30
        else -> Long.MAX_VALUE
      }
    }
  }

  @VisibleForTesting
  fun filterByJobStatus(
    connectorRollout: ConnectorRollout,
    candidates: Collection<ConfigScopeMapWithId>,
    actorDefinitionConnections: List<StandardSync>,
    actorType: ActorType,
  ): List<ConfigScopeMapWithId> {
    // If any of the actor's connections failed we don't want to use the actor for the rollout
    val actorJobInfo = getActorJobInfo(connectorRollout, actorDefinitionConnections, actorType, null, null)
    return candidates.filter {
      actorJobInfo.containsKey(it.id) && actorJobInfo[it.id]!!.nFailed == 0 && actorJobInfo[it.id]!!.nSucceeded > 0
    }
  }

  @VisibleForTesting
  fun getUniqueActorIds(
    sortedActorDefinitionConnections: List<StandardSync>,
    nActorsToPin: Int,
    actorType: ActorType,
  ): List<UUID> {
    // Get a list of unique actor IDs, maintaining the sort order
    val uniqueActorIds =
      sortedActorDefinitionConnections
        .map { if (actorType == ActorType.SOURCE) it.sourceId else it.destinationId }
        .toSet()

    return uniqueActorIds.toList().take(nActorsToPin)
  }
}
