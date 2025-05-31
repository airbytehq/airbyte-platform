/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.shared

import io.airbyte.config.ActorType
import io.airbyte.config.AttributeName
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectionSummary
import io.airbyte.config.ConnectionWithLatestJob
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFilters
import io.airbyte.config.CustomerTier
import io.airbyte.config.CustomerTierFilter
import io.airbyte.config.Job
import io.airbyte.config.JobBypassFilter
import io.airbyte.config.JobConfig
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.JobStatus
import io.airbyte.config.JobSyncConfig
import io.airbyte.config.Operator
import io.airbyte.config.Schedule
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.JobService
import io.airbyte.data.services.OrganizationCustomerAttributesService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ActorWorkspaceOrganizationIds
import io.airbyte.data.services.shared.ConfigScopeMapWithId
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import java.time.OffsetDateTime
import java.util.UUID

class RolloutActorFinderTest {
  private val actorDefinitionVersionUpdater = mockk<ActorDefinitionVersionUpdater>()
  private val connectionService = mockk<ConnectionService>()
  private val jobService = mockk<JobService>()
  private val scopedConfigurationService = mockk<ScopedConfigurationService>()
  private val sourceService = mockk<SourceService>()
  private val destinationService = mockk<DestinationService>()
  private val organizationCustomerAttributesService = mockk<OrganizationCustomerAttributesService>()
  private val actorDefinitionService = mockk<ActorDefinitionService>()
  private val rolloutActorFinder =
    RolloutActorFinder(
      actorDefinitionVersionUpdater,
      connectionService,
      jobService,
      scopedConfigurationService,
      sourceService,
      destinationService,
      organizationCustomerAttributesService,
      actorDefinitionService,
    )

  companion object {
    private const val TARGET_PERCENTAGE = 50
    private val SOURCE_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val DESTINATION_ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val RELEASE_CANDIDATE_VERSION_ID = UUID.randomUUID()
    private val SOURCE_ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID()
    private val DESTINATION_ACTOR_DEFINITION_VERSION_ID = UUID.randomUUID()

    private val ORGANIZATION_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_1_ACTOR_ID = UUID.randomUUID()
    private val ORGANIZATION_1_WORKSPACE_2_ACTOR_ID = UUID.randomUUID()

    private val ORGANIZATION_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_ID_1 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_ID_2 = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_1_ACTOR_ID = UUID.randomUUID()
    private val ORGANIZATION_2_WORKSPACE_2_ACTOR_ID = UUID.randomUUID()

    private val CONFIG_SCOPE_MAP =
      mapOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_1_ACTOR_ID,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID to
          ConfigScopeMapWithId(
            id = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_1_WORKSPACE_2_ACTOR_ID,
                ConfigScopeType.WORKSPACE to ORGANIZATION_1_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_1_ACTOR_ID to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_1_ACTOR_ID,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_1_ACTOR_ID,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_1,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1,
              ),
          ),
        ORGANIZATION_2_WORKSPACE_2_ACTOR_ID to
          ConfigScopeMapWithId(
            id = ORGANIZATION_2_WORKSPACE_2_ACTOR_ID,
            scopeMap =
              mapOf(
                ConfigScopeType.ACTOR to ORGANIZATION_2_WORKSPACE_2_ACTOR_ID,
                ConfigScopeType.WORKSPACE to ORGANIZATION_2_WORKSPACE_ID_2,
                ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2,
              ),
          ),
      )

    private fun createConnectionsFromConfigScopeMap(
      configScopeMap: Map<UUID, ConfigScopeMapWithId>,
      actorType: ActorType,
    ): List<ConnectionSummary> =
      configScopeMap.map { actor ->
        ConnectionSummary(
          connectionId = UUID.randomUUID(),
          manual = false,
          schedule = null,
          sourceId = if (actorType == ActorType.SOURCE) actor.key else UUID.randomUUID(),
          destinationId = if (actorType == ActorType.DESTINATION) actor.key else UUID.randomUUID(),
        )
      }

    private fun createJobsFromConnectionSummaries(
      syncs: List<ConnectionSummary>,
      nFailure: Int,
      nPinnedToReleaseCandidate: Int,
    ): List<Job> =
      syncs.mapIndexed { index, connection ->
        val jobStatus = if (index < nFailure) JobStatus.FAILED else JobStatus.SUCCEEDED
        Job(
          0,
          ConfigType.SYNC,
          connection.connectionId.toString(),
          JobConfig().apply {
            sync =
              JobSyncConfig().apply {
                sourceDockerImageIsDefault =
                  if (index < nPinnedToReleaseCandidate) {
                    false
                  } else {
                    true
                  }
                sourceDefinitionVersionId =
                  if (index < nPinnedToReleaseCandidate) {
                    RELEASE_CANDIDATE_VERSION_ID
                  } else {
                    SOURCE_ACTOR_DEFINITION_VERSION_ID
                  }
                destinationDockerImageIsDefault =
                  if (index < nPinnedToReleaseCandidate) {
                    false
                  } else {
                    true
                  }
                destinationDefinitionVersionId =
                  if (index < nPinnedToReleaseCandidate) {
                    RELEASE_CANDIDATE_VERSION_ID
                  } else {
                    DESTINATION_ACTOR_DEFINITION_VERSION_ID
                  }
              }
          },
          emptyList(),
          jobStatus,
          0L,
          0L,
          0L,
          true,
        )
      }

    private fun createMockConnectorRollout(actorDefinitionId: UUID): ConnectorRollout =
      ConnectorRollout(
        id = UUID.randomUUID(),
        actorDefinitionId = actorDefinitionId,
        releaseCandidateVersionId = RELEASE_CANDIDATE_VERSION_ID,
        initialVersionId = UUID.randomUUID(),
        state = ConnectorEnumRolloutState.INITIALIZED,
        initialRolloutPct = 10,
        finalTargetRolloutPct = TARGET_PERCENTAGE,
        hasBreakingChanges = false,
        rolloutStrategy = ConnectorEnumRolloutStrategy.MANUAL,
        maxStepWaitTimeMins = 60,
        createdAt = OffsetDateTime.now().toEpochSecond(),
        updatedAt = OffsetDateTime.now().toEpochSecond(),
        expiresAt = OffsetDateTime.now().plusDays(1).toEpochSecond(),
        tag = null,
      )

    private fun jobWithVersionAndDefaultFlag(
      actorType: ActorType,
      versionId: UUID?,
      isDefault: Boolean,
      scope: String = UUID.randomUUID().toString(),
      createdAt: Long = System.currentTimeMillis(),
      status: JobStatus = JobStatus.SUCCEEDED,
    ): Job {
      val syncConfig =
        JobSyncConfig().apply {
          if (actorType == ActorType.SOURCE) {
            sourceDefinitionVersionId = versionId
            sourceDockerImageIsDefault = isDefault
          } else {
            destinationDefinitionVersionId = versionId
            destinationDockerImageIsDefault = isDefault
          }
        }

      val jobConfig =
        JobConfig().apply {
          sync = syncConfig
        }

      return Job(
        0,
        ConfigType.SYNC,
        scope,
        jobConfig,
        emptyList(),
        status,
        createdAt,
        createdAt,
        createdAt,
        true,
      )
    }
  }

  @BeforeEach
  fun reset() {
    clearAllMocks()
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo`(actorType: ActorType) {
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any()) } returns mockConnectionSyncs
    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returnsMany
      listOf(
        createJobsFromConnectionSummaries(mockConnectionSyncs, 0, 0),
        // Second call returns empty, ending pagination
        emptyList(),
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo =
      rolloutActorFinder.getActorSelectionInfo(
        createMockConnectorRollout(actorDefinitionId),
        TARGET_PERCENTAGE,
        null,
      )

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(CONFIG_SCOPE_MAP.size * TARGET_PERCENTAGE / 100, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(2, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo with null percentage to pin`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any()) } returns mockConnectionSyncs
    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returnsMany
      listOf(
        createJobsFromConnectionSummaries(mockConnectionSyncs, 0, 0),
        emptyList(),
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), null, null)

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(0, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(0, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @Test
  fun `test getTargetTotalToPin`() {
    val mockConnectorRollout = createMockConnectorRollout(SOURCE_ACTOR_DEFINITION_ID)

    // No eligible actors, nothing previously pinned, no targetPercentage
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 0, 0, 1))
    // No eligible actors, nothing previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 0, 0, 1))
    // No eligible actors, one previously pinned, no targetPercentage
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 0, 1, 0))
    // No eligible actors, one previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 0, 1, 1))

    // 1 eligible, 0 previously pinned
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 1, 0, 1))
    // 1 eligible, one previously pinned
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 1, 1, 1))

    // 2 eligible, 0 previously pinned, targetPercentage 1
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 2, 0, 1))
    // 2 eligible, 0 previously pinned, targetPercentage 100
    assertEquals(2, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 2, 0, 100))
    // 2 eligible, 1 previously pinned, targetPercentage 1
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 2, 1, 1))
    // 2 eligible, 1 previously pinned, targetPercentage 50
    assertEquals(0, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 2, 1, 50))
    // 2 eligible, 1 previously pinned, targetPercentage 70
    assertEquals(1, rolloutActorFinder.getTargetTotalToPin(mockConnectorRollout, 2, 1, 70))
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getSyncInfoForPinnedActors`(actorType: ActorType) {
    val connectorRolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(connectorRolloutId)
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      // Two actors are pinned to the RC
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = actorDefinitionId
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = actorDefinitionId
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        // One actor is pinned to a version that is not the RC
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = actorDefinitionId
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = ORGANIZATION_1_WORKSPACE_2_ACTOR_ID
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )
    every {
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(
        any(),
        any(),
        any(),
      )
    } returns mockConnectionSyncs

    val paginatedJobBatches =
      mockConnectionSyncs
        .map { connection ->
          Job(
            0,
            ConfigType.SYNC,
            connection.connectionId.toString(),
            JobConfig().apply {
              sync =
                JobSyncConfig().apply {
                  sourceDockerImageIsDefault = true
                  sourceDefinitionVersionId =
                    if (actorType == ActorType.SOURCE) {
                      RELEASE_CANDIDATE_VERSION_ID
                    } else {
                      SOURCE_ACTOR_DEFINITION_VERSION_ID
                    }
                  destinationDockerImageIsDefault = true
                  destinationDefinitionVersionId =
                    if (actorType == ActorType.DESTINATION) {
                      RELEASE_CANDIDATE_VERSION_ID
                    } else {
                      DESTINATION_ACTOR_DEFINITION_VERSION_ID
                    }
                }
            },
            emptyList(),
            JobStatus.SUCCEEDED,
            0L,
            0L,
            0L,
            true,
          )
        }.chunked(1000)

    every { jobService.findLatestJobPerScope(any(), any(), any()) } returnsMany (
      paginatedJobBatches + listOf(emptyList()) // Add an empty list at the end to stop pagination
    )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()
    every { actorDefinitionService.getIdsForActors(any()) } returns
      CONFIG_SCOPE_MAP.values
        .map {
          ActorWorkspaceOrganizationIds(
            it.scopeMap[ConfigScopeType.ACTOR]!!,
            it.scopeMap[ConfigScopeType.WORKSPACE]!!,
            it.scopeMap[ConfigScopeType.ORGANIZATION]!!,
          )
        }.take(2)

    val syncInfo = rolloutActorFinder.getSyncInfoForPinnedActors(connectorRollout)

    // Two actors are pinned
    assertEquals(2, syncInfo.size)

    verify {
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
    }
  }

  @Test
  fun `updateActorSyncJobInfo updates counters correctly`() {
    val info = ActorSyncJobInfo(nSucceeded = 0, nFailed = 0, nConnections = 0)
    rolloutActorFinder.updateActorSyncJobInfo(info, createJobWithStatusVersion(JobStatus.SUCCEEDED))
    rolloutActorFinder.updateActorSyncJobInfo(info, createJobWithStatusVersion(JobStatus.FAILED))
    rolloutActorFinder.updateActorSyncJobInfo(info, createJobWithStatusVersion(JobStatus.CANCELLED))
    assertEquals(1, info.nSucceeded)
    assertEquals(1, info.nFailed)
    assertEquals(3, info.nConnections)
  }

  private fun createJobWithStatusVersion(
    status: JobStatus = JobStatus.SUCCEEDED,
    sourceVersion: UUID = UUID.randomUUID(),
    destinationVersion: UUID = UUID.randomUUID(),
    sourceVersionIsDefault: Boolean = false,
    destinationVersionIsDefault: Boolean = false,
  ): Job =
    Job(
      0,
      ConfigType.SYNC,
      UUID.randomUUID().toString(),
      JobConfig().apply {
        sync =
          JobSyncConfig().apply {
            sourceDockerImageIsDefault = sourceVersionIsDefault
            sourceDefinitionVersionId = sourceVersion
            destinationDockerImageIsDefault = destinationVersionIsDefault
            destinationDefinitionVersionId = destinationVersion
          }
      },
      emptyList(),
      status,
      0L,
      0L,
      0L,
      true,
    )

  private fun createMockConnectionWithJob(
    sourceVersionId: UUID,
    destinationVersionId: UUID,
    jobStatus: JobStatus,
  ): ConnectionWithLatestJob =
    ConnectionWithLatestJob(
      connection = ConnectionSummary(UUID.randomUUID(), true, null, sourceVersionId, destinationVersionId),
      job = createJobWithStatusVersion(jobStatus, sourceVersion = sourceVersionId, destinationVersion = destinationVersionId),
    )

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorJobInfo`(actorType: ActorType) {
    val versionId: UUID
    val connection1: ConnectionWithLatestJob
    val connection2: ConnectionWithLatestJob
    val connection3: ConnectionWithLatestJob

    if (actorType ==
      ActorType.SOURCE
    ) {
      versionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
      connection1 = createMockConnectionWithJob(SOURCE_ACTOR_DEFINITION_VERSION_ID, UUID.randomUUID(), JobStatus.SUCCEEDED)
      connection2 = createMockConnectionWithJob(SOURCE_ACTOR_DEFINITION_VERSION_ID, UUID.randomUUID(), JobStatus.FAILED)
      // wrong version
      connection3 = createMockConnectionWithJob(UUID.randomUUID(), UUID.randomUUID(), JobStatus.SUCCEEDED)
    } else {
      versionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
      connection1 = createMockConnectionWithJob(UUID.randomUUID(), DESTINATION_ACTOR_DEFINITION_VERSION_ID, JobStatus.SUCCEEDED)
      connection2 = createMockConnectionWithJob(UUID.randomUUID(), DESTINATION_ACTOR_DEFINITION_VERSION_ID, JobStatus.FAILED)
      // wrong version
      connection3 = createMockConnectionWithJob(UUID.randomUUID(), UUID.randomUUID(), JobStatus.SUCCEEDED)
    }
    val connectorRollout = createMockConnectorRollout(UUID.randomUUID())
    val result =
      rolloutActorFinder.getActorJobInfo(
        connectorRollout,
        listOf(connection1, connection2, connection3),
        actorType,
        versionId,
      )
    // One actor pinned to release candidate
    assertEquals(1, result.size)
    result.values.forEach {
      assertEquals(2, it.nConnections)
    }
    val succeeded = result.values.firstOrNull { it.nSucceeded == 1 }
    val failed = result.values.firstOrNull { it.nFailed == 1 }
    assertNotNull(succeeded)
    assertNotNull(failed)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorJobInfo does not list jobs if there are no connections`(actorType: ActorType) {
    val connectorRolloutId = UUID.randomUUID()
    val connectorRollout = createMockConnectorRollout(connectorRolloutId)

    val jobInfo =
      rolloutActorFinder.getActorJobInfo(
        connectorRollout,
        emptyList(),
        actorType,
        connectorRollout.releaseCandidateVersionId,
      )

    assertEquals(0, jobInfo.size)

    verify(exactly = 0) { jobService.findLatestJobPerScope(any(), any(), any()) }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test jobDefinitionVersionIdEq`(actorType: ActorType) {
    val actorDefinitionVersionId: UUID
    if (actorType == ActorType.SOURCE) {
      actorDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
    } else {
      actorDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
    }
    val job1 =
      Job(
        1,
        ConfigType.SYNC,
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID.toString(),
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDefinitionVersionId = SOURCE_ACTOR_DEFINITION_VERSION_ID
              destinationDefinitionVersionId = DESTINATION_ACTOR_DEFINITION_VERSION_ID
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )

    val job2 =
      Job(
        2,
        ConfigType.SYNC,
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID.toString(),
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDefinitionVersionId = UUID.randomUUID()
              destinationDefinitionVersionId = UUID.randomUUID()
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )

    assertEquals(true, rolloutActorFinder.jobDefinitionVersionIdEq(actorType, job1, actorDefinitionVersionId))
    assertEquals(false, rolloutActorFinder.jobDefinitionVersionIdEq(actorType, job2, actorDefinitionVersionId))
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test jobDockerImageIsDefault`(actorType: ActorType) {
    val job1 =
      Job(
        1,
        ConfigType.SYNC,
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID.toString(),
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDockerImageIsDefault = true
              destinationDockerImageIsDefault = true
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )

    val job2 =
      Job(
        2,
        ConfigType.SYNC,
        ORGANIZATION_1_WORKSPACE_2_ACTOR_ID.toString(),
        JobConfig().apply {
          sync =
            JobSyncConfig().apply {
              sourceDockerImageIsDefault = false
              destinationDockerImageIsDefault = false
            }
        },
        emptyList(),
        JobStatus.SUCCEEDED,
        0L,
        0L,
        0L,
        true,
      )

    assertEquals(true, rolloutActorFinder.jobDockerImageIsDefault(actorType, job1))
    assertEquals(false, rolloutActorFinder.jobDockerImageIsDefault(actorType, job2))
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo actorIdsToPin is rounded up no filters`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every { connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any()) } returns mockConnectionSyncs
    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returnsMany
      listOf(
        createJobsFromConnectionSummaries(mockConnectionSyncs, 0, 0),
        emptyList(),
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), 1, null)

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(1, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo actorIdsToPin with jobBypassFilter`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every { connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any()) } returns mockConnectionSyncs
    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returnsMany
      listOf(
        createJobsFromConnectionSummaries(mockConnectionSyncs, 0, 0),
        emptyList(),
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo =
      rolloutActorFinder.getActorSelectionInfo(
        createMockConnectorRollout(actorDefinitionId),
        1,
        ConnectorRolloutFilters(jobBypassFilter = JobBypassFilter(AttributeName.BYPASS_JOBS, false)),
      )

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(1, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo with bypass filter`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns listOf()
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo =
      rolloutActorFinder.getActorSelectionInfo(
        createMockConnectorRollout(actorDefinitionId),
        1,
        ConnectorRolloutFilters(jobBypassFilter = JobBypassFilter(AttributeName.BYPASS_JOBS, true)),
      )

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    verify(exactly = 0) {
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
    }

    assertEquals(1, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(1, actorSelectionInfo.nNewPinned)
    assertEquals(0, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorSelectionInfo with previously pinned`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val mockConnectionSyncs = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
      every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
        listOf(
          ScopedConfiguration().apply {
            id = UUID.randomUUID()
            key = "key1"
            value = RELEASE_CANDIDATE_VERSION_ID.toString()
            resourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
            resourceType = ConfigResourceType.SOURCE
            scopeId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
            scopeType = ConfigScopeType.ACTOR
            originType = ConfigOriginType.CONNECTOR_ROLLOUT
          },
        )
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
      every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
        listOf(
          ScopedConfiguration().apply {
            id = UUID.randomUUID()
            key = "key1"
            value = RELEASE_CANDIDATE_VERSION_ID.toString()
            resourceId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
            resourceType = ConfigResourceType.DESTINATION
            scopeId = ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
            scopeType = ConfigScopeType.ACTOR
            originType = ConfigOriginType.CONNECTOR_ROLLOUT
          },
        )
    }
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet() -
      setOf(
        ORGANIZATION_1_WORKSPACE_1_ACTOR_ID,
      )
    every { scopedConfigurationService.getScopedConfigurations(any(), any(), any(), any()) } returns mapOf()
    every { connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any()) } returns mockConnectionSyncs
    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returnsMany
      listOf(
        createJobsFromConnectionSummaries(mockConnectionSyncs, 0, 1),
        emptyList(),
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns emptyMap()

    val actorSelectionInfo = rolloutActorFinder.getActorSelectionInfo(createMockConnectorRollout(actorDefinitionId), 1, null)

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
      actorDefinitionVersionUpdater.getConfigScopeMaps(any())
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
      scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any())
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(any(), any(), any())
      jobService.findLatestJobPerScope(any(), any(), any())
      organizationCustomerAttributesService.getOrganizationTiers()
    }

    // We already exceed the target percentage so shouldn't pin something new
    assertEquals(0, actorSelectionInfo.actorIdsToPin.size)
    assertEquals(4, actorSelectionInfo.nActors)
    assertEquals(4, actorSelectionInfo.nActorsEligibleOrAlreadyPinned)
    assertEquals(0, actorSelectionInfo.nNewPinned)
    assertEquals(1, actorSelectionInfo.nPreviouslyPinned)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorType`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } returns StandardSourceDefinition()
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } returns StandardDestinationDefinition()
    }

    rolloutActorFinder.getActorType(actorDefinitionId)

    verify {
      if (actorType == ActorType.SOURCE) {
        sourceService.getStandardSourceDefinition(any())
      } else {
        destinationService.getStandardDestinationDefinition(any())
      }
    }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorType throws`(actorType: ActorType) {
    if (actorType == ActorType.SOURCE) {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } throws ConfigNotFoundException("", "Not found")
    } else {
      every { sourceService.getStandardSourceDefinition(any()) } throws ConfigNotFoundException("", "Not found")
      every { destinationService.getStandardDestinationDefinition(any()) } throws ConfigNotFoundException("", "Not found")
    }

    assertThrows<IllegalStateException> { rolloutActorFinder.getActorType(UUID.randomUUID()) }

    verify {
      sourceService.getStandardSourceDefinition(any())
      destinationService.getStandardDestinationDefinition(any())
    }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByTier excludes organizations when filter is present`(actorType: ActorType) {
    val organizationTiers =
      mapOf(
        ORGANIZATION_ID_1 to CustomerTier.TIER_0,
        ORGANIZATION_ID_2 to CustomerTier.TIER_2,
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns organizationTiers

    val candidates =
      listOf(
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to null),
        ),
      )

    val filteredCandidates =
      rolloutActorFinder.filterByTier(
        createMockConnectorRollout(UUID.randomUUID()),
        candidates,
        listOf(
          CustomerTierFilter(
            AttributeName.TIER,
            Operator.IN,
            listOf(CustomerTier.TIER_2),
          ),
        ),
      )

    assertEquals(1, filteredCandidates.size)
    assertTrue(filteredCandidates.any { it.scopeMap[ConfigScopeType.ORGANIZATION] == ORGANIZATION_ID_2 })

    verify { organizationCustomerAttributesService.getOrganizationTiers() }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByTier excludes no organizations when no filter is present`(actorType: ActorType) {
    val organizationTiers =
      mapOf(
        ORGANIZATION_ID_1 to CustomerTier.TIER_0,
        ORGANIZATION_ID_2 to CustomerTier.TIER_2,
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns organizationTiers

    val candidates =
      listOf(
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to null),
        ),
      )

    val filteredCandidates =
      rolloutActorFinder.filterByTier(
        createMockConnectorRollout(UUID.randomUUID()),
        candidates,
        emptyList(),
      )

    assertEquals(3, filteredCandidates.size)

    verify { organizationCustomerAttributesService.getOrganizationTiers() }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByTier includes organizations with null tier with Tier 2`(actorType: ActorType) {
    val organizationTiers =
      mapOf(
        ORGANIZATION_ID_1 to null,
        ORGANIZATION_ID_2 to CustomerTier.TIER_2,
      )
    every { organizationCustomerAttributesService.getOrganizationTiers() } returns organizationTiers

    val candidates =
      listOf(
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_1),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to ORGANIZATION_ID_2),
        ),
        ConfigScopeMapWithId(
          id = UUID.randomUUID(),
          scopeMap = mapOf(ConfigScopeType.ORGANIZATION to null),
        ),
      )

    val filteredCandidates =
      rolloutActorFinder.filterByTier(
        createMockConnectorRollout(UUID.randomUUID()),
        candidates,
        listOf(
          CustomerTierFilter(
            AttributeName.TIER,
            Operator.IN,
            listOf(CustomerTier.TIER_2),
          ),
        ),
      )

    assertEquals(2, filteredCandidates.size)

    verify { organizationCustomerAttributesService.getOrganizationTiers() }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorsPinnedToReleaseCandidate no actors pinned`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns emptyList()

    assertEquals(
      0,
      rolloutActorFinder
        .getActorsPinnedToReleaseCandidate(
          createMockConnectorRollout(actorDefinitionId),
        ).size,
    )

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorsPinnedToReleaseCandidate scopes pinned to RC`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = RELEASE_CANDIDATE_VERSION_ID.toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )

    assertEquals(2, rolloutActorFinder.getActorsPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)).size)

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getActorsPinnedToReleaseCandidate scopes not pinned to RC`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    // Two scopes are pinned, but not to the RC
    every { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) } returns
      listOf(
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = SOURCE_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
        ScopedConfiguration().apply {
          id = UUID.randomUUID()
          key = "key1"
          value = UUID.randomUUID().toString()
          resourceId = DESTINATION_ACTOR_DEFINITION_ID
          resourceType = ConfigResourceType.ACTOR_DEFINITION
          scopeId = UUID.randomUUID()
          scopeType = ConfigScopeType.ACTOR
          originType = ConfigOriginType.CONNECTOR_ROLLOUT
        },
      )

    assertEquals(0, rolloutActorFinder.getActorsPinnedToReleaseCandidate(createMockConnectorRollout(actorDefinitionId)).size)

    verify { scopedConfigurationService.listScopedConfigurationsWithValues(any(), any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByAlreadyPinned no actors pinned returns all`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet()
    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values

    val candidates = rolloutActorFinder.filterByAlreadyPinned(actorDefinitionId, CONFIG_SCOPE_MAP.values)

    verify {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    }

    assertEquals(CONFIG_SCOPE_MAP.values.toSet(), candidates.toSet())
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByAlreadyPinned filters pinned actors`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID

    every { actorDefinitionVersionUpdater.getConfigScopeMaps(any()) } returns CONFIG_SCOPE_MAP.values
    every {
      actorDefinitionVersionUpdater.getUpgradeCandidates(any(), any())
    } returns CONFIG_SCOPE_MAP.map { it.key }.toSet() -
      setOf(ORGANIZATION_1_WORKSPACE_1_ACTOR_ID)

    val candidates = rolloutActorFinder.filterByAlreadyPinned(actorDefinitionId, CONFIG_SCOPE_MAP.values)

    assertEquals(
      CONFIG_SCOPE_MAP
        .filter {
          it.key != ORGANIZATION_1_WORKSPACE_1_ACTOR_ID
        }.values
        .toSet(),
      candidates.toSet(),
    )
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test getSortedConnectionsWithLatestJob`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val connectorRollout = createMockConnectorRollout(actorDefinitionId)

    val connectionIds = List(5) { UUID.randomUUID() }
    val actorIds = List(3) { UUID.randomUUID() }

    val mockConnectionSummaries =
      connectionIds.mapIndexed { index, connectionId ->
        val schedule =
          when (index) {
            0 ->
              Schedule().apply {
                timeUnit = Schedule.TimeUnit.MINUTES
                units = 1
              }
            1 ->
              Schedule().apply {
                timeUnit = Schedule.TimeUnit.HOURS
                units = 2
              }
            // null schedule should go last
            2 -> null
            3 ->
              Schedule().apply {
                timeUnit = Schedule.TimeUnit.DAYS
                units = 1
              }
            else -> null
          }
        // this one should be filtered because it's manual
        val manual = (index == 4)

        ConnectionSummary(
          connectionId = connectionId,
          manual = manual,
          schedule = schedule,
          sourceId = if (actorType == ActorType.SOURCE) actorIds[index % actorIds.size] else UUID.randomUUID(),
          destinationId = if (actorType == ActorType.DESTINATION) actorIds[index % actorIds.size] else UUID.randomUUID(),
        )
      }

    val connectionSummaryMap = mockConnectionSummaries.associateBy { it.connectionId }

    val mockJobs =
      connectionSummaryMap.mapValues { (_, conn) ->
        jobWithVersionAndDefaultFlag(actorType, null, true, scope = conn.connectionId.toString())
      }

    val mockConnectionWithJobs =
      mockConnectionSummaries.map {
        ConnectionWithLatestJob(it, mockJobs[it.connectionId])
      }

    // Mock the connection summary fetch
    every {
      connectionService.listConnectionSummaryByActorDefinitionIdAndActorIds(
        actorDefinitionId,
        actorType.value(),
        any(),
      )
    } returns mockConnectionSummaries

    // Spy on the rolloutActorFinder and stub getConnectionsWithLatestJob
    val spyFinder = spyk(rolloutActorFinder)
    every {
      spyFinder.getConnectionsWithLatestJob(
        connectorRollout,
        connectionSummaryMap,
        actorType,
        any(),
        null,
      )
    } returns mockConnectionWithJobs

    val result =
      spyFinder.getSortedConnectionsWithLatestJob(
        connectorRollout,
        actorIds,
        actorDefinitionId,
        actorType,
        includeManual = false,
        versionId = null,
      )

    // Should exclude the manual connection
    assertEquals(4, result.size)

    // Should be sorted in order of frequency (low minutes first, nulls last)
    val frequencies = result.map { it.connection.schedule }.map { rolloutActorFinder.getFrequencyInMinutes(it) }
    assertTrue(frequencies == frequencies.sorted(), "Connections should be sorted by frequency")

    assertFalse(result.any { it.connection.manual == true }, "Should exclude manual connections")
  }

  @Test
  fun `test getFrequencyInMinutes with different schedule values`() {
    var schedule: Schedule? = null
    var expectedMinutes = Long.MAX_VALUE
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is MINUTES and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.MINUTES
        units = 10
      }
    expectedMinutes = 10
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is HOURS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.HOURS
        units = 2
      }
    expectedMinutes = 2 * 60
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is DAYS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.DAYS
        units = 3
      }
    expectedMinutes = 3 * 60 * 24
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is WEEKS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.WEEKS
        units = 1
      }
    expectedMinutes = 1 * 60 * 24 * 7
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when timeUnit is MONTHS and units is not null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.MONTHS
        units = 1
      }
    expectedMinutes = 1 * 60 * 24 * 30
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))

    // Test when units is null
    schedule =
      Schedule().apply {
        timeUnit = Schedule.TimeUnit.DAYS
        units = null
      }
    expectedMinutes = Long.MAX_VALUE
    assertEquals(expectedMinutes, rolloutActorFinder.getFrequencyInMinutes(schedule))
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `test filterByJobStatus`(actorType: ActorType) {
    val actorDefinitionId = if (actorType == ActorType.SOURCE) SOURCE_ACTOR_DEFINITION_ID else DESTINATION_ACTOR_DEFINITION_ID
    val mockConnectionSummaries = createConnectionsFromConfigScopeMap(CONFIG_SCOPE_MAP, actorType)

    val succeededJob =
      jobWithVersionAndDefaultFlag(
        actorType = actorType,
        versionId = null,
        isDefault = true,
        scope = mockConnectionSummaries.first().connectionId.toString(),
        status = JobStatus.SUCCEEDED,
      )

    val emptyJobs =
      mockConnectionSummaries.drop(1).map {
        jobWithVersionAndDefaultFlag(
          actorType = actorType,
          versionId = null,
          isDefault = true,
          scope = it.connectionId.toString(),
          // won't count toward success
          status = JobStatus.INCOMPLETE,
        )
      }

    val allJobs = listOf(succeededJob) + emptyJobs

    every {
      jobService.findLatestJobPerScope(any(), any(), any())
    } returns allJobs

    val jobMap = allJobs.associateBy { UUID.fromString(it.scope) }
    val connectionsWithLatestJob =
      mockConnectionSummaries.map {
        ConnectionWithLatestJob(it, jobMap[it.connectionId])
      }

    val candidates =
      rolloutActorFinder.filterByJobStatus(
        connectorRollout = createMockConnectorRollout(actorDefinitionId),
        candidates = CONFIG_SCOPE_MAP.values,
        connectionsWithLatestJob = connectionsWithLatestJob,
        actorType = actorType,
      )

    assertEquals(1, candidates.size, "Only the connection with the succeeded job should pass the filter")
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `sortActorIdsBySyncFrequency returns actorIds in sync order and skips duplicates`(actorType: ActorType) {
    val actor1 = UUID.randomUUID()
    val actor2 = UUID.randomUUID()
    val actor3 = UUID.randomUUID()

    val candidates =
      listOf(
        ConfigScopeMapWithId(actor1, emptyMap()),
        ConfigScopeMapWithId(actor2, emptyMap()),
        ConfigScopeMapWithId(actor3, emptyMap()),
      )

    val connections: List<ConnectionWithLatestJob>
    if (actorType == ActorType.SOURCE) {
      connections =
        listOf(
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, actor1, UUID.randomUUID()), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, actor2, UUID.randomUUID()), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, actor1, UUID.randomUUID()), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, actor3, UUID.randomUUID()), null),
        )
    } else {
      connections =
        listOf(
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, UUID.randomUUID(), actor1), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, UUID.randomUUID(), actor2), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, UUID.randomUUID(), actor1), null),
          ConnectionWithLatestJob(ConnectionSummary(UUID.randomUUID(), false, null, UUID.randomUUID(), actor3), null),
        )
    }

    val sorted = rolloutActorFinder.sortActorIdsBySyncFrequency(candidates, connections, actorType)

    assertEquals(listOf(actor1, actor2, actor3), sorted)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `isJobCompatibleWithVersion returns true if versionId matches`(actorType: ActorType) {
    val version = UUID.randomUUID()
    val job = jobWithVersionAndDefaultFlag(actorType, version, false)

    val result = rolloutActorFinder.isJobCompatibleWithVersion(actorType, job, version)

    assertTrue(result)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `isJobCompatibleWithVersion returns false if versionId does not match`(actorType: ActorType) {
    val job = jobWithVersionAndDefaultFlag(actorType, UUID.randomUUID(), false)

    val result = rolloutActorFinder.isJobCompatibleWithVersion(actorType, job, UUID.randomUUID())

    assertFalse(result)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `isJobCompatibleWithVersion returns true if no versionId and docker image is default`(actorType: ActorType) {
    val job = jobWithVersionAndDefaultFlag(actorType, UUID.randomUUID(), true)

    val result = rolloutActorFinder.isJobCompatibleWithVersion(actorType, job, null)

    assertTrue(result)
  }

  @ParameterizedTest
  @EnumSource(ActorType::class)
  fun `isJobCompatibleWithVersion returns true if no versionId and docker image is not default`(actorType: ActorType) {
    val job = jobWithVersionAndDefaultFlag(actorType, UUID.randomUUID(), false)

    val result = rolloutActorFinder.isJobCompatibleWithVersion(actorType, job, null)

    assertTrue(result)
  }

  @Test
  fun `getUniqueActorIds returns all when fewer than requested`() {
    val actorId1 = UUID.randomUUID()
    val actorId2 = UUID.randomUUID()
    // contains duplicate
    val sortedActorIds = listOf(actorId1, actorId2, actorId1)

    val result = rolloutActorFinder.getUniqueActorIds(sortedActorIds, 5)

    assertEquals(listOf(actorId1, actorId2), result, "Should return only available unique actor IDs, preserving order.")
  }

  @Test
  fun `getUniqueActorIds maintains order of first occurrence`() {
    val actorId1 = UUID.randomUUID()
    val actorId2 = UUID.randomUUID()
    val actorId3 = UUID.randomUUID()

    val sortedActorIds = listOf(actorId2, actorId1, actorId2, actorId3)

    val result = rolloutActorFinder.getUniqueActorIds(sortedActorIds, 3)

    assertEquals(listOf(actorId2, actorId1, actorId3), result, "Order of first appearance should be preserved.")
  }

  @ParameterizedTest
  @CsvSource("1", "2", "3", "4")
  fun `getUniqueActorIds respects nActorsToPin limit`(nActorsToPin: Int) {
    val ids = List(4) { UUID.randomUUID() }
    val sortedActorIds = listOf(ids[0], ids[1], ids[2], ids[3], ids[0], ids[2])

    val result = rolloutActorFinder.getUniqueActorIds(sortedActorIds, nActorsToPin)

    val expected = ids.take(nActorsToPin)
    assertEquals(expected, result, "Should return at most nActorsToPin unique actor IDs in original order.")
  }
}
