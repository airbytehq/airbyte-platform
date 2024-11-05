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
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
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
  val percentagePinned: Int,
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
) {
  fun getActorSelectionInfo(
    connectorRollout: ConnectorRollout,
    targetPercent: Int,
  ): ActorSelectionInfo {
    logger.info { "Finding actors to pin for rollout ${connectorRollout.id}" }

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

    // Calculate the number to pin based on the input percentage
    val targetTotalToPin = ceil(nEligibleOrAlreadyPinned * targetPercent / 100.0).toInt()

    // From the eligible actors, choose the ones with the next sync
    // TODO: filter out those with lots of data
    // TODO: prioritize internal connections
    val actorIdsToPin =
      getUniqueActorIds(
        sortedActorDefinitionConnections.filter { candidates.map { it.id }.contains(it.sourceId ?: it.destinationId) },
        targetTotalToPin - nPreviouslyPinned,
        actorType,
      )

    logger.info { "Rollout ${connectorRollout.id}: targetTotalToPin=$targetTotalToPin; pinning ${actorIdsToPin.size} actors" }

    return ActorSelectionInfo(
      actorIdsToPin = actorIdsToPin,
      nActors = initialNCandidates,
      nActorsEligibleOrAlreadyPinned = nEligibleOrAlreadyPinned,
      nNewPinned = actorIdsToPin.size,
      nPreviouslyPinned = nPreviouslyPinned,
      // Total percentage pinned out of all eligible, including new and previously pinned
      // This could end up being >100% if the number of eligible actors changes between rollout increments.
      percentagePinned =
        if (nEligibleOrAlreadyPinned == 0) {
          0
        } else {
          ceil((nPreviouslyPinned + actorIdsToPin.size) * 100.0 / nEligibleOrAlreadyPinned).toInt()
        },
    )
  }

  fun getSyncInfoForPinnedActors(connectorRollout: ConnectorRollout): Map<UUID, ActorSyncJobInfo> {
    val actorType = getActorType(connectorRollout.actorDefinitionId)

    val pinnedActorSyncs =
      getSortedActorDefinitionConnections(
        getActorsPinnedToReleaseCandidate(connectorRollout),
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
    val actorActorSyncJobInfoMap = mutableMapOf<UUID, ActorSyncJobInfo>()

    for (connection in actorSyncs) {
      val actorId = (if (actorType == ActorType.SOURCE) connection.sourceId else connection.destinationId) ?: continue
      val connectionJobs =
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
        ).filter { if (versionId != null) jobDefinitionVersionIdEq(actorType, it, versionId) else jobDockerImageIsDefault(actorType, it) }
      val nSucceeded = connectionJobs.filter { it.status == JobStatus.SUCCEEDED }.size
      val nFailed = connectionJobs.filter { it.status == JobStatus.FAILED }.size

      if (actorActorSyncJobInfoMap.containsKey(actorId)) {
        actorActorSyncJobInfoMap[actorId]!!.nSucceeded += nSucceeded
        actorActorSyncJobInfoMap[actorId]!!.nFailed += nSucceeded
      } else {
        actorActorSyncJobInfoMap[actorId] = ActorSyncJobInfo(nSucceeded = nSucceeded, nFailed = nFailed)
      }
      actorActorSyncJobInfoMap[actorId]!!.nConnections++
    }

    logger.info { "connectorRollout.id=${connectorRollout.id} actorActorSyncJobInfoMap=$actorActorSyncJobInfoMap" }

    return actorActorSyncJobInfoMap
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
    // TODO - filter out the ineligible actors (workspace in the list of tier 0/1 customers)
    // Query from https://airbytehq-team.slack.com/archives/C06AZD64PDJ/p1727885479202429?thread_ts=1727885477.845219&cid=C06AZD64PDJ -
    // val priorityWorkspaces =
    // SELECT
    //  w.workspace_id,
    //  w.account_id,
    //  sc.customer_tier
    // FROM
    //  airbyte-data-prod.airbyte_warehouse.workspace w
    // JOIN
    //  airbyte-data-prod.airbyte_warehouse.support_case sc
    // ON
    //  w.account_id = sc.account_id
    // WHERE
    //  lower(sc.customer_tier) IN ('tier 1', 'tier 0')
    //
    // candidates = candidates.filter {
    //   !priorityWorkspaces.contains(it.workspaceId)
    // }
    return candidates
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

    return scopedConfigurations.filter {
      it.value == connectorRollout.releaseCandidateVersionId.toString() &&
        it.originType == ConfigOriginType.CONNECTOR_ROLLOUT
    }.map { it.id }
  }

  @VisibleForTesting
  fun getSortedActorDefinitionConnections(
    actorIds: List<UUID>,
    actorDefinitionId: UUID,
    actorType: ActorType,
  ): List<StandardSync> {
    return connectionService.listConnectionsByActorDefinitionIdAndType(
      actorDefinitionId,
      actorType.toString(),
      false,
    ).filter { connection ->
      when (actorType) {
        ActorType.SOURCE -> connection.sourceId in actorIds
        ActorType.DESTINATION -> connection.destinationId in actorIds
      }
    }.filter { connection ->
      connection.manual != true
    }.sortedBy { connection ->
      getFrequencyInMinutes(connection.schedule)
    }
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
