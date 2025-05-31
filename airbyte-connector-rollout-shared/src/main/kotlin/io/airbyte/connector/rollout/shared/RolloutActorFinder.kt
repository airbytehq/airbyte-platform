/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import io.airbyte.config.ActorType
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectionSummary
import io.airbyte.config.ConnectionWithLatestJob
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFilters
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.Job
import io.airbyte.config.JobConfig
import io.airbyte.config.JobStatus
import io.airbyte.config.Schedule
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.annotation.Trace
import jakarta.inject.Singleton
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.math.ceil

private val logger = KotlinLogging.logger {}
private const val SCOPE_FETCH_LIMIT = 1000

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
  private val actorDefinitionService: ActorDefinitionService,
) {
  /**
   * Get information about actors that are eligible for the rollout.
   *
   * This information includes:
   * - The number of eligible actors.
   * - The number of actors already pinned to the release candidate.
   * - If a target percent is passed in, also gets a list of the actors to pin.
   *
   * Actors are "eligible" if:
   * - They are in the tier specified by the CustomerTier filter.
   * - They have at least one connection that's had a sync in the past day.
   * - The most recent sync for each of their connections succeeded.
   * - They are using the default version of the connector (i.e. aren't pinned to a non-default image).
   */
  @Trace
  fun getActorSelectionInfo(
    connectorRollout: ConnectorRollout,
    targetPercent: Int?,
    filters: ConnectorRolloutFilters?,
  ): ActorSelectionInfo {
    logger.info { "Finding actors to pin for rollout ${connectorRollout.id} targetPercent=$targetPercent" }

    var candidates = actorDefinitionVersionUpdater.getConfigScopeMaps(connectorRollout.actorDefinitionId)
    val initialNCandidates = candidates.size

    logger.info { "Rollout ${connectorRollout.id}: $initialNCandidates actors total" }

    val actorType = getActorType(connectorRollout.actorDefinitionId)
    candidates = filterByTier(connectorRollout, candidates, filters?.customerTierFilters)

    logger.info { "Rollout ${connectorRollout.id}: ${candidates.size} after filterByTier" }

    var sortedConnectionsWithLatestJob: List<ConnectionWithLatestJob>? = null
    if (filters?.jobBypassFilter == null || filters.jobBypassFilter?.evaluate(Unit) == false) {
      val sortedConnectionsWithLatestJob =
        getSortedConnectionsWithLatestJob(connectorRollout, candidates.map { it.id }, connectorRollout.actorDefinitionId, actorType, false, null)
      candidates = filterByJobStatus(connectorRollout, candidates, sortedConnectionsWithLatestJob, actorType)
    }

    logger.info {
      "Rollout ${connectorRollout.id}: ${candidates.size} after filterByJobStatus"
    }

    val nPreviouslyPinned = getActorsPinnedToReleaseCandidate(connectorRollout).filter { candidates.map { it.id }.contains(it) }.size
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
    val targetTotalToPin = getTargetTotalToPin(connectorRollout, nEligibleOrAlreadyPinned, nPreviouslyPinned, targetPercent)
    logger.info {
      "Rollout ${connectorRollout.id}: candidates.size=$candidates.size "
    }

    // From the eligible actors, choose the ones with the next sync
    // TODO: filter out those with lots of data
    // TODO: prioritize internal connections
    val actorIdsToPin =
      getUniqueActorIds(
        sortActorIdsBySyncFrequency(candidates, sortedConnectionsWithLatestJob, actorType),
        targetTotalToPin,
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

  internal fun getTargetTotalToPin(
    connectorRollout: ConnectorRollout,
    nEligibleOrAlreadyPinned: Int,
    nPreviouslyPinned: Int,
    targetPercentage: Int,
  ): Int {
    logger.info {
      "connectorRollout=${connectorRollout.id} " +
        "nEligibleOrAlreadyPinned=$nEligibleOrAlreadyPinned " +
        "nPreviouslyPinned=$nPreviouslyPinned " +
        "targetPercentage=$targetPercentage"
    }
    if (nEligibleOrAlreadyPinned == 0 || targetPercentage <= 0) {
      return 0
    }

    val targetTotal = ceil(nEligibleOrAlreadyPinned.toDouble() * targetPercentage / 100).toInt()
    logger.info { "connectorRollout=${connectorRollout.id} targetTotal=$targetTotal" }

    if (nPreviouslyPinned >= targetTotal) {
      return 0
    }

    val actorsToPin = targetTotal - nPreviouslyPinned

    return minOf(actorsToPin, nEligibleOrAlreadyPinned - nPreviouslyPinned)
  }

  /**
   * Get information about syncs performed by actors pinned to the release candidate.
   *
   * This will be used to determine whether the release should be rolled forward/back, so we look at jobs for all
   * connections with a pinned actor (including those that were run manually).
   */
  fun getSyncInfoForPinnedActors(connectorRollout: ConnectorRollout): Map<UUID, ActorSyncJobInfo> {
    val actorType = getActorType(connectorRollout.actorDefinitionId)

    var pinnedActorIds = getActorsPinnedToReleaseCandidate(connectorRollout)
    logger.info { "Pinned actors: connectorRollout=${connectorRollout.id} pinnedActors.size=$pinnedActorIds.size" }

    pinnedActorIds = filterByTier(connectorRollout, pinnedActorIds)
    logger.info {
      "Pinned actors after filterByTier: " +
        "connectorRollout=${connectorRollout.id} pinnedActors.size=$pinnedActorIds.size " +
        "tier=${connectorRollout.filters?.customerTierFilters?.map { it.value }}"
    }

    val pinnedActorConnectionsWithLatestJob =
      getSortedConnectionsWithLatestJob(
        connectorRollout,
        pinnedActorIds,
        connectorRollout.actorDefinitionId,
        actorType,
        true,
        connectorRollout.releaseCandidateVersionId,
      )

    logger.info { "Found ${pinnedActorConnectionsWithLatestJob.size} connections for pinned actors. connectorRollout.id=${connectorRollout.id}" }

    return getActorJobInfo(
      connectorRollout,
      pinnedActorConnectionsWithLatestJob,
      actorType,
      connectorRollout.releaseCandidateVersionId,
    )
  }

  @Trace
  internal fun getActorJobInfo(
    connectorRollout: ConnectorRollout,
    connectionsWithLatestJob: List<ConnectionWithLatestJob>,
    actorType: ActorType,
    versionId: UUID?,
  ): Map<UUID, ActorSyncJobInfo> {
    val actorSyncJobInfoMap = mutableMapOf<UUID, ActorSyncJobInfo>()
    val connectionIdToActorId = mapConnectionToActor(connectionsWithLatestJob, actorType)

    logger.info {
      "Getting actorSyncJobInfo connectorRollout.id=${connectorRollout.id} " +
        "connectionsWithLatestJob.size=${connectionsWithLatestJob.size} versionId=$versionId"
    }

    if (connectionIdToActorId.isEmpty()) {
      return actorSyncJobInfoMap
    }

    connectionsWithLatestJob.forEach { connectionWithJob ->
      val connection = connectionWithJob.connection
      val actorId = connectionIdToActorId[connection.connectionId]

      if (actorId == null) {
        logger.debug {
          "ActorID not found in connectionIdToActorId " +
            "connectorRollout.id=${connectorRollout.id} actorId=$actorId connectionId=${connection.connectionId}"
        }
        return@forEach
      }

      val job = connectionWithJob.job
      if (job == null || !isJobCompatibleWithVersion(actorType, job, versionId)) {
        logger.debug {
          "Skipping job for connectorRollout.id=${connectorRollout.id} " +
            "actorId=$actorId connectionId=${connection.connectionId} expected versionId=$versionId."
        }
        return@forEach
      }

      val nSucceeded = if (job.status == JobStatus.SUCCEEDED) 1 else 0
      val nFailed = if (job.status == JobStatus.FAILED) 1 else 0

      logger.debug {
        "connectorRollout.id=${connectorRollout.id} actorId=$actorId latestJob.status=${job.status} " +
          "nSucceeded=$nSucceeded nFailed=$nFailed"
      }

      val jobInfo = actorSyncJobInfoMap.getOrPut(actorId) { ActorSyncJobInfo() }
      updateActorSyncJobInfo(jobInfo, job)
    }

    logger.info { "connectorRollout.id=${connectorRollout.id} actorActorSyncJobInfoMap=$actorSyncJobInfoMap" }
    return actorSyncJobInfoMap
  }

  internal fun mapConnectionToActor(
    connectionWithLatestJob: List<ConnectionWithLatestJob>,
    actorType: ActorType,
  ): Map<UUID, UUID> =
    connectionWithLatestJob.associate {
      val actorId = if (actorType == ActorType.SOURCE) it.connection.sourceId else it.connection.destinationId
      it.connection.connectionId to actorId
    }

  internal fun updateActorSyncJobInfo(
    jobInfo: ActorSyncJobInfo,
    job: Job,
  ) {
    if (job.status == JobStatus.SUCCEEDED) jobInfo.nSucceeded++
    if (job.status == JobStatus.FAILED) jobInfo.nFailed++
    jobInfo.nConnections++
  }

  internal fun jobDefinitionVersionIdEq(
    actorType: ActorType,
    job: Job,
    versionId: UUID,
  ): Boolean =
    if (actorType == ActorType.SOURCE) {
      job.config.sync.sourceDefinitionVersionId == versionId
    } else {
      job.config.sync.destinationDefinitionVersionId == versionId
    }

  internal fun jobDockerImageIsDefault(
    actorType: ActorType,
    job: Job,
  ): Boolean =
    if (actorType == ActorType.SOURCE) {
      job.config.sync.sourceDockerImageIsDefault
    } else {
      job.config.sync.destinationDockerImageIsDefault
    }

  internal fun getActorType(actorDefinitionId: UUID): ActorType =
    try {
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

  @Trace
  internal fun filterByTier(
    connectorRollout: ConnectorRollout,
    candidates: Collection<ConfigScopeMapWithId>,
    filters: List<CustomerTierFilter>?,
  ): Collection<ConfigScopeMapWithId> {
    val organizationTiers = organizationCustomerAttributesService.getOrganizationTiers()
    logger.debug { "RolloutActorFinder.filterByTier: connectorRollout.id=${connectorRollout.id} organizationTiers=$organizationTiers" }
    if (filters.isNullOrEmpty()) {
      logger.debug { "RolloutActorFinder.filterByTier: connectorRollout.id=${connectorRollout.id} no tier filter specified." }
      return candidates
    } else {
      logger.debug { "RolloutActorFinder.filterByTier: connectorRollout.id=${connectorRollout.id} applying tier filter for filters=$filters" }
      return candidates.filter { candidate ->
        val organizationId = candidate.scopeMap[ConfigScopeType.ORGANIZATION] ?: return@filter false

        val tier = organizationTiers[organizationId]
        filters.all { filter ->
          // Evaluate the filter based on the organization's tier:
          // - If the tier is known, evaluate it normally.
          // - If the tier is unknown but the filter allows TIER_2, treat it as a match.
          // - Otherwise, the filter fails.
          when {
            tier != null -> filter.evaluate(tier)
            filter.value.contains(CustomerTier.TIER_2) -> true
            else -> false
          }
        }
      }
    }
  }

  internal fun filterByTier(
    connectorRollout: ConnectorRollout,
    actorIds: List<UUID>,
  ): List<UUID> =
    filterByTier(
      connectorRollout,
      actorDefinitionService.getIdsForActors(actorIds).map {
        ConfigScopeMapWithId(
          it.actorId,
          mapOf(
            ConfigScopeType.ACTOR to it.actorId,
            ConfigScopeType.WORKSPACE to it.workspaceId,
            ConfigScopeType.ORGANIZATION to it.organizationId,
          ),
        )
      },
      connectorRollout.filters?.customerTierFilters,
    ).map { it.id }

  @Trace
  internal fun filterByAlreadyPinned(
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

  @Trace
  internal fun getActorsPinnedToReleaseCandidate(connectorRollout: ConnectorRollout): List<UUID> {
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
      scopedConfigurations
        .filter {
          it.value == connectorRollout.releaseCandidateVersionId.toString() &&
            it.originType == ConfigOriginType.CONNECTOR_ROLLOUT
        }.map { it.scopeId }

    logger.info { "getActorsPinnedToReleaseCandidate  connectorRollout.id=${connectorRollout.id} filtered=$filtered" }
    return filtered
  }

  @Trace
  internal fun getSortedConnectionsWithLatestJob(
    connectorRollout: ConnectorRollout,
    actorIds: List<UUID>,
    actorDefinitionId: UUID,
    actorType: ActorType,
    includeManual: Boolean,
    versionId: UUID?,
  ): List<ConnectionWithLatestJob> {
    logger.info {
      "Getting sorted actor definition connections for actor Ids. " +
        "connectorRollout.id=${connectorRollout.id} actorIds=${actorIds.size} actorDefinitionId=$actorDefinitionId actorType=$actorType"
    }

    val allConnections: List<ConnectionWithLatestJob> =
      actorIds.chunked(SCOPE_FETCH_LIMIT).withIndex().flatMap { (actorIdBatchIndex, batchActorIds) ->

        logger.info {
          "Fetching connection & job info for ${batchActorIds.size} actorIds. " +
            "connectorRollout.id=${connectorRollout.id} actorIdBatchIndex=$actorIdBatchIndex"
        }

        val batchConnections =
          connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(
            actorDefinitionId,
            actorType.value(),
            batchActorIds,
          )

        val batchConnectionSummariesWithLatestJob =
          getConnectionsWithLatestJob(
            connectorRollout,
            batchConnections.associateBy { it.connectionId },
            actorType,
            Instant.ofEpochMilli(connectorRollout.createdAt).atOffset(ZoneOffset.UTC),
            versionId,
          )

        logger.info {
          "Fetched connection summary & latest job for batch. " +
            "connectorRollout.id=${connectorRollout.id} " +
            "actorIdBatchIndex=$actorIdBatchIndex " +
            "batchActorIds.size=${batchActorIds.size} " +
            "batchConnections.size=${batchConnections.size} " +
            "batchConnectionSummariesWithLatestJob.size=${batchConnectionSummariesWithLatestJob.size}"
        }

        batchConnectionSummariesWithLatestJob
      }

    logger.info {
      "Done fetching connections. connectorRollout.id=${connectorRollout.id} allConnections.size=${allConnections.size}"
    }
    val sortedConnections =
      allConnections
        .filter { connectionWithLatestJob ->
          when (actorType) {
            ActorType.SOURCE -> connectionWithLatestJob.connection.sourceId in actorIds
            ActorType.DESTINATION -> connectionWithLatestJob.connection.destinationId in actorIds
          }
        }.filter { connectionWithLatestJob ->
          includeManual || connectionWithLatestJob.connection.manual != true
        }.sortedBy { connectionWithLatestJob ->
          getFrequencyInMinutes(connectionWithLatestJob.connection.schedule)
        }

    logger.info {
      "Done filtering connections. connectorRollout.id=${connectorRollout.id} sortedConnections.size=${sortedConnections.size}"
    }

    return sortedConnections
  }

  @Trace
  internal fun getConnectionsWithLatestJob(
    connectorRollout: ConnectorRollout,
    connectionSummaryMap: Map<UUID, ConnectionSummary>,
    actorType: ActorType,
    updatedAt: OffsetDateTime?,
    versionId: UUID?,
  ): List<ConnectionWithLatestJob> {
    if (connectionSummaryMap.isEmpty()) return emptyList()

    val connectionIdsAsStrings = connectionSummaryMap.keys.map(UUID::toString).toSet()

    return buildList {
      fetchAndProcessJobsForConnections(
        connectorRollout = connectorRollout,
        configTypes = setOf(JobConfig.ConfigType.SYNC),
        connectionIds = connectionIdsAsStrings,
        createdAt = updatedAt,
      ) { jobsByConnectionId ->

        jobsByConnectionId.forEach { (connectionId, job) ->
          if (!isJobCompatibleWithVersion(actorType, job, versionId)) {
            logger.debug {
              "Skipping job with version != $versionId. connectorRollout.id=${connectorRollout.id} connectionId=$connectionId"
            }
            return@forEach
          }

          connectionSummaryMap[connectionId]?.let { summary ->
            add(ConnectionWithLatestJob(summary, job))
          }
        }
      }
    }
  }

  private fun fetchAndProcessJobsForConnections(
    connectorRollout: ConnectorRollout,
    configTypes: Set<JobConfig.ConfigType>,
    connectionIds: Set<String>,
    createdAt: OffsetDateTime?,
    processJobs: (Map<UUID, Job>) -> Unit,
  ) {
    connectionIds.chunked(SCOPE_FETCH_LIMIT).forEachIndexed { batchIndex, batch ->
      logger.info {
        "Getting jobs for a batch of connections. " +
          "connectorRollout.id=${connectorRollout.id} job batchIndex=$batchIndex job batch.size=${batch.size}"
      }

      val jobs =
        jobService.findLatestJobPerScope(
          configTypes = configTypes,
          scopes = batch.toSet(),
          createdAtStart = createdAt ?: OffsetDateTime.now().minusDays(1),
        )

      val jobsByConnectionId = jobs.associateBy { UUID.fromString(it.scope) }
      processJobs(jobsByConnectionId)
    }
  }

  internal fun getFrequencyInMinutes(schedule: Schedule?): Long =
    if (schedule?.units == null) {
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

  internal fun isJobCompatibleWithVersion(
    actorType: ActorType,
    job: Job,
    versionId: UUID?,
  ): Boolean =
    versionId?.let {
      jobDefinitionVersionIdEq(actorType, job, it)
    } ?: true

  internal fun filterByJobStatus(
    connectorRollout: ConnectorRollout,
    candidates: Collection<ConfigScopeMapWithId>,
    connectionsWithLatestJob: List<ConnectionWithLatestJob>,
    actorType: ActorType,
  ): List<ConfigScopeMapWithId> {
    // If any of the actor's connections failed we don't want to use the actor for the rollout
    val actorJobInfo = getActorJobInfo(connectorRollout, connectionsWithLatestJob, actorType, null)
    logger.info { "Got Actor job info. connectorRollout=${connectorRollout.id} actorJobInfo=$actorJobInfo" }

    return candidates.filter { candidate ->
      val jobInfo = actorJobInfo[candidate.id]
      logger.debug { "connectorRollout=${connectorRollout.id} Candidate ID: ${candidate.id}, Job Info: $jobInfo" }

      val passesFilter = jobInfo != null && jobInfo.nFailed == 0 && jobInfo.nSucceeded > 0
      logger.debug { "connectorRollout=${connectorRollout.id} Candidate ${candidate.id} passes filter: $passesFilter" }

      passesFilter
    }
  }

  internal fun sortActorIdsBySyncFrequency(
    candidates: Collection<ConfigScopeMapWithId>,
    sortedConnectionsWithLatestJob: List<ConnectionWithLatestJob>?,
    actorType: ActorType,
  ): List<UUID> {
    val candidateIds = candidates.map { it.id }.toSet()

    if (sortedConnectionsWithLatestJob == null) {
      return candidateIds.toList()
    }

    val seen = mutableSetOf<UUID>()

    return sortedConnectionsWithLatestJob.mapNotNull { conn ->
      val actorId = if (actorType == ActorType.SOURCE) conn.connection.sourceId else conn.connection.destinationId
      if (actorId in candidateIds && seen.add(actorId)) actorId else null
    }
  }

  internal fun getUniqueActorIds(
    sortedActorIds: List<UUID>,
    nActorsToPin: Int,
  ): List<UUID> = sortedActorIds.distinct().take(nActorsToPin)
}
