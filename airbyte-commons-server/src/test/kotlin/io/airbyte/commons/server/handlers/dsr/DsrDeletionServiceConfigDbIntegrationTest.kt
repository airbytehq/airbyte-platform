/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.dsr

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.temporal.ConnectionManagerUtils
import io.airbyte.config.AuthUser
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.DestinationConnection
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSync
import io.airbyte.config.User
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.Permission
import io.airbyte.data.repositories.entities.Workspace
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorCatalogType
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.airbyte.db.instance.configs.jooq.generated.enums.NamespaceDefinitionType
import io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
import io.airbyte.db.instance.configs.jooq.generated.enums.Status
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType
import io.airbyte.domain.services.dsr.DsrDeletionRequestTimeoutService
import io.airbyte.domain.services.dsr.DsrManifest
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.DbPrune
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import io.airbyte.config.AuthProvider as ConfigAuthProvider
import io.airbyte.db.instance.configs.jooq.generated.Tables as ConfigTables
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as JooqAuthProvider

internal class DsrDeletionServiceConfigDbIntegrationTest : BaseConfigDatabaseTest() {
  private lateinit var userPersistence: UserPersistence
  private lateinit var workspaceRepository: WorkspaceRepository
  private lateinit var organizationRepository: OrganizationRepository
  private lateinit var connectionService: ConnectionService
  private lateinit var sourceService: SourceService
  private lateinit var destinationService: DestinationService
  private lateinit var connectorBuilderService: ConnectorBuilderService
  private lateinit var permissionRepository: PermissionRepository
  private lateinit var externalUserService: ExternalUserService
  private lateinit var connectionManagerUtils: ConnectionManagerUtils
  private lateinit var sourceHandler: SourceHandler
  private lateinit var destinationHandler: DestinationHandler
  private lateinit var dbPrune: DbPrune
  private lateinit var deletionRequestRepository: DataSubjectDeletionRequestRepository
  private lateinit var deletionRequestTimeoutService: DsrDeletionRequestTimeoutService
  private lateinit var metricClient: MetricClient
  private lateinit var objectMapper: ObjectMapper
  private lateinit var executionHeartbeatExecutor: ScheduledExecutorService
  private lateinit var executionHeartbeatFuture: ScheduledFuture<*>
  private lateinit var service: TestDsrDeletionService

  private val email = "davin.integration@airbyte.io"
  private val datagrailId = "dg-integration-123"
  private val oncallIssueNumber = "ONCALL-4321"
  private val executedBy = "reviewer@airbyte.io"
  private val emailHash = DsrDeletionService.emailHash(email)

  private val userId = UUID.randomUUID()
  private val coMemberUserId = UUID.randomUUID()
  private val organizationId = UUID.randomUUID()
  private val dataplaneGroupId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val actorCatalogId = UUID.randomUUID()
  private val actorCatalogFetchEventId = UUID.randomUUID()
  private val connectionId = UUID.randomUUID()
  private val stateId = UUID.randomUUID()
  private val builderProjectId = UUID.randomUUID()
  private val tombstonedBuilderProjectId = UUID.randomUUID()
  private val permissionId = UUID.randomUUID()
  private val authUserRowId = UUID.randomUUID()
  private val applicationId = UUID.randomUUID()
  private val scopedConfigurationId = UUID.randomUUID()
  private val legacySyncWorkloadId = "${connectionId}_123_0_sync"
  private val orphanedSyncWorkloadId = "${connectionId}_999_0_sync"
  private val actorCheckWorkloadId = "${sourceId}_456_0_check"
  private val actorDefinitionDiscoverWorkloadId = "${sourceDefinitionId}_789_discover"
  private val unrelatedActorDefinitionDiscoverWorkloadId = "${sourceDefinitionId}_999_discover"
  private val commandId = "dsr-command-${UUID.randomUUID()}"
  private val observabilityJobId = UUID.randomUUID().mostSignificantBits
  private val dataWorkerUsageReservationJobId = UUID.randomUUID().mostSignificantBits

  private val unrelatedUserId = UUID.randomUUID()
  private val unrelatedOrganizationId = UUID.randomUUID()
  private val unrelatedDataplaneGroupId = UUID.randomUUID()
  private val unrelatedWorkspaceId = UUID.randomUUID()
  private val unrelatedActorId = UUID.randomUUID()
  private val unrelatedApplicationId = UUID.randomUUID()
  private val unrelatedScopedConfigurationId = UUID.randomUUID()
  private val unrelatedWorkloadId = "${UUID.randomUUID()}_123_0_sync"
  private val unrelatedCommandId = "unrelated-command-${UUID.randomUUID()}"
  private val unrelatedObservabilityJobId = UUID.randomUUID().mostSignificantBits
  private val unrelatedDataWorkerUsageReservationJobId = UUID.randomUUID().mostSignificantBits

  private data class FinalizedExecution(
    val finalStatus: DataSubjectDeletionStatus,
    val scrubbedEmail: String,
    val scrubbedManifest: String,
    val confirmErrors: String?,
    val executionCounts: String,
  )

  @BeforeEach
  fun setup() {
    truncateAllTables()

    userPersistence = mockk(relaxed = true)
    workspaceRepository = mockk(relaxed = true)
    organizationRepository = mockk(relaxed = true)
    connectionService = mockk(relaxed = true)
    sourceService = mockk(relaxed = true)
    destinationService = mockk(relaxed = true)
    connectorBuilderService = mockk(relaxed = true)
    permissionRepository = mockk(relaxed = true)
    externalUserService = mockk(relaxed = true)
    connectionManagerUtils = mockk(relaxed = true)
    sourceHandler = mockk(relaxed = true)
    destinationHandler = mockk(relaxed = true)
    dbPrune = mockk(relaxed = true)
    deletionRequestRepository = mockk(relaxed = true)
    deletionRequestTimeoutService = mockk(relaxed = true)
    metricClient = mockk(relaxed = true)
    objectMapper = jacksonObjectMapper()
    executionHeartbeatExecutor = mockk(relaxed = true)
    executionHeartbeatFuture = mockk(relaxed = true)
    every { deletionRequestRepository.markRunningExecutionStarted(any(), any()) } returns 1
    every { deletionRequestRepository.heartbeatRunningExecution(any(), any()) } returns 1
    every { deletionRequestRepository.finalizeRunningExecutionIfActive(any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { deletionRequestRepository.failRunningExecutionIfRunning(any(), any(), any(), any(), any(), any()) } returns 1
    every {
      executionHeartbeatExecutor.scheduleAtFixedRate(any(), any(), any(), TimeUnit.MILLISECONDS)
    } returns executionHeartbeatFuture
    every { executionHeartbeatFuture.cancel(false) } returns true

    seedConfigDatabase()
    stubManifestDependencies()

    service = TestDsrDeletionService()
  }

  private fun captureActiveFinalization(requestId: UUID): MutableList<FinalizedExecution> {
    val finalizations = mutableListOf<FinalizedExecution>()
    every {
      deletionRequestRepository.finalizeRunningExecutionIfActive(requestId, any(), any(), any(), any(), any(), any())
    } answers {
      val args = invocation.args
      finalizations.add(
        FinalizedExecution(
          finalStatus = args[1] as DataSubjectDeletionStatus,
          scrubbedEmail = args[3] as String,
          scrubbedManifest = args[4] as String,
          confirmErrors = args[5] as String?,
          executionCounts = args[6] as String,
        ),
      )
      1
    }
    return finalizations
  }

  private inner class TestDsrDeletionService :
    DsrDeletionService(
      userPersistence = userPersistence,
      workspaceRepository = workspaceRepository,
      organizationRepository = organizationRepository,
      connectionService = connectionService,
      sourceService = sourceService,
      destinationService = destinationService,
      connectorBuilderService = connectorBuilderService,
      permissionRepository = permissionRepository,
      externalUserService = externalUserService,
      connectionManagerUtils = connectionManagerUtils,
      sourceHandler = sourceHandler,
      destinationHandler = destinationHandler,
      dbPrune = dbPrune,
      deletionRequestRepository = deletionRequestRepository,
      deletionRequestTimeoutService = deletionRequestTimeoutService,
      objectMapper = objectMapper,
      metricClient = metricClient,
      configDatabase = database!!,
      executionHeartbeatExecutor = executionHeartbeatExecutor,
    ) {
    fun hardDeleteConfigRowsForTest(
      ctx: DSLContext,
      manifest: DsrManifest,
      datagrailId: String,
      syncWorkloadIds: List<String>,
    ): ConfigDeletionCounts = hardDeleteConfigRows(ctx, manifest, datagrailId, syncWorkloadIds)
  }

  @Test
  fun `execute hard-deletes captured config scopes and preserves unrelated rows in the migrated config database`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = "support@airbyte.io",
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
        prepareWarnings = objectMapper.writeValueAsString(listOf("reviewed $email")),
      )
    val finalizations = captureActiveFinalization(requestId)

    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.markRunningIfPreviewed(requestId, email, datagrailId, oncallIssueNumber, executedBy, any())
    } answers {
      row.status = DataSubjectDeletionStatus.running
      row.confirmedBy = executedBy
      1
    }
    every { sourceHandler.deleteSource(any<SourceIdRequestBody>()) } just Runs
    every { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) } just Runs
    every { dbPrune.listSyncWorkloadIdsByScopes(listOf(connectionId.toString())) } returns listOf(legacySyncWorkloadId)
    every { dbPrune.pruneJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobDeletionCounts(deletedJobsCount = 3, deletedAttemptsCount = 4)
    every { externalUserService.deleteUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns 1

    val startExecutionResult = service.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    val result = service.executeClaimedRequest(startExecutionResult.requestId)

    assertEquals("COMPLETED", result.status)
    assertEquals(3, result.deletedJobsCount)
    assertEquals(4, result.deletedAttemptsCount)
    assertEquals(1, result.deletedConnectionsCount)
    assertEquals(2, result.deletedActorsCount)
    assertEquals(2, result.deletedBuilderProjectsCount)
    assertEquals(1, result.deletedPermissionsCount)
    assertEquals(1, result.deletedWorkspacesCount)
    assertEquals(1, result.deletedOrganizationsCount)
    assertEquals(1, result.deletedAuthUsersCount)
    assertTrue(result.tombstonedUser)
    assertEquals(1, result.deletedKeycloakUserCount)
    assertEquals(3, objectMapper.readTree(finalizations.single().executionCounts).get("deleted_jobs_count").asInt())

    assertDeleted("connection", connectionId)
    assertDeleted("actor", sourceId)
    assertDeleted("actor", destinationId)
    assertDeleted("connector_builder_project", builderProjectId)
    assertDeleted("connector_builder_project", tombstonedBuilderProjectId)
    assertDeleted("permission", permissionId)
    assertDeleted("auth_user", authUserRowId)
    assertDeleted("application", applicationId)
    assertDeleted("state", stateId)
    assertDeleted("actor_catalog_fetch_event", actorCatalogFetchEventId)
    assertDeleted("scoped_configuration", scopedConfigurationId)
    assertDeletedLong("observability_jobs_stats", "job_id", observabilityJobId)
    assertDeletedLong("observability_stream_stats", "job_id", observabilityJobId)
    assertDeletedUuidField("data_worker_usage", "organization_id", organizationId)
    assertDeletedLong("data_worker_usage_reservation", "job_id", dataWorkerUsageReservationJobId)
    assertDeletedString("workload", "id", legacySyncWorkloadId)
    assertDeletedString("workload_queue", "workload_id", legacySyncWorkloadId)
    assertDeletedString("workload", "id", orphanedSyncWorkloadId)
    assertDeletedString("workload_queue", "workload_id", orphanedSyncWorkloadId)
    assertDeletedString("workload", "id", actorCheckWorkloadId)
    assertDeletedString("workload_queue", "workload_id", actorCheckWorkloadId)
    assertDeletedString("workload", "id", actorDefinitionDiscoverWorkloadId)
    assertDeletedString("workload_queue", "workload_id", actorDefinitionDiscoverWorkloadId)
    assertDeletedString("commands", "id", commandId)
    assertDeleted("workspace", workspaceId)
    assertDeleted("organization", organizationId)

    assertPresent("actor_catalog", actorCatalogId)
    assertPresent("user", userId)
    assertPresent("user", coMemberUserId)
    assertPresent("workspace", unrelatedWorkspaceId)
    assertPresent("organization", unrelatedOrganizationId)
    assertPresent("actor", unrelatedActorId)
    assertPresent("application", unrelatedApplicationId)
    assertPresent("scoped_configuration", unrelatedScopedConfigurationId)
    assertPresentLong("observability_jobs_stats", "job_id", unrelatedObservabilityJobId)
    assertPresentLong("observability_stream_stats", "job_id", unrelatedObservabilityJobId)
    assertPresentUuidField("data_worker_usage", "organization_id", unrelatedOrganizationId)
    assertPresentLong("data_worker_usage_reservation", "job_id", unrelatedDataWorkerUsageReservationJobId)
    assertPresentString("workload", "id", unrelatedWorkloadId)
    assertPresentString("workload", "id", unrelatedActorDefinitionDiscoverWorkloadId)
    assertPresentString("workload_queue", "workload_id", unrelatedActorDefinitionDiscoverWorkloadId)
    assertPresentString("commands", "id", unrelatedCommandId)

    val userRow =
      database!!.query { ctx: DSLContext ->
        ctx
          .select(ConfigTables.USER.EMAIL, ConfigTables.USER.NAME, ConfigTables.USER.DEFAULT_WORKSPACE_ID)
          .from(ConfigTables.USER)
          .where(ConfigTables.USER.ID.eq(userId))
          .fetchOne()
      }
    assertEquals(datagrailId, userRow!!.get(ConfigTables.USER.EMAIL))
    assertEquals(datagrailId, userRow.get(ConfigTables.USER.NAME))
    assertNull(userRow.get(ConfigTables.USER.DEFAULT_WORKSPACE_ID))

    val coMemberUserRow =
      database!!.query { ctx: DSLContext ->
        ctx
          .select(ConfigTables.USER.EMAIL, ConfigTables.USER.DEFAULT_WORKSPACE_ID)
          .from(ConfigTables.USER)
          .where(ConfigTables.USER.ID.eq(coMemberUserId))
          .fetchOne()
      }
    assertEquals("co.member.integration@airbyte.io", coMemberUserRow!!.get(ConfigTables.USER.EMAIL))
    assertNull(coMemberUserRow.get(ConfigTables.USER.DEFAULT_WORKSPACE_ID))

    val finalization = finalizations.single()
    assertEquals(DataSubjectDeletionStatus.completed, finalization.finalStatus)
    assertEquals(datagrailId, finalization.scrubbedEmail)
    assertNull(finalization.confirmErrors)
    assertFalse(finalization.scrubbedManifest.contains(email))
    assertFalse(finalization.scrubbedManifest.contains("Davin Integration"))
    assertFalse(finalization.scrubbedManifest.contains("postgres integration"))
    assertFalse(finalization.scrubbedManifest.contains("bigquery integration"))

    verify(exactly = 1) { sourceHandler.deleteSource(any<SourceIdRequestBody>()) }
    verify(exactly = 1) { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) }
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
  }

  @Test
  fun `workload prefix range only deletes underscore-prefixed workload ids at the C collation boundary`() {
    val matchingWorkloadId = "${connectionId}_boundary_0_sync"
    val lowerNeighborWorkloadId = "$connectionId^boundary_0_sync"
    val upperNeighborWorkloadId = "$connectionId`boundary_0_sync"
    val boundaryManifest =
      emptyManifestForUser(userId).copy(
        workspaceIds = listOf(workspaceId),
        workspaceRefs = listOf(DsrManifest.ManifestWorkspace(workspaceId, "dsr-workspace")),
        organizationIds = listOf(organizationId),
        organizationRefs = listOf(DsrManifest.ManifestOrganization(organizationId, "dsr-org")),
        connectionIds = listOf(connectionId),
      )

    database!!.query { ctx: DSLContext ->
      insertWorkload(ctx, matchingWorkloadId, workspaceId, organizationId, "sync")
      insertWorkloadQueue(ctx, matchingWorkloadId, dataplaneGroupId)
      insertWorkload(ctx, lowerNeighborWorkloadId, workspaceId, organizationId, "sync")
      insertWorkloadQueue(ctx, lowerNeighborWorkloadId, dataplaneGroupId)
      insertWorkload(ctx, upperNeighborWorkloadId, workspaceId, organizationId, "sync")
      insertWorkloadQueue(ctx, upperNeighborWorkloadId, dataplaneGroupId)
    }

    database!!.transaction { ctx: DSLContext ->
      service.hardDeleteConfigRowsForTest(ctx, boundaryManifest, datagrailId, emptyList())
    }

    assertDeletedString("workload", "id", matchingWorkloadId)
    assertDeletedString("workload_queue", "workload_id", matchingWorkloadId)
    assertPresentString("workload", "id", lowerNeighborWorkloadId)
    assertPresentString("workload_queue", "workload_id", lowerNeighborWorkloadId)
    assertPresentString("workload", "id", upperNeighborWorkloadId)
    assertPresentString("workload_queue", "workload_id", upperNeighborWorkloadId)
  }

  @Test
  fun `hard delete removes data worker usage for workspace scope when organization is not in scope`() {
    val workspaceOnlyOrganizationId = UUID.randomUUID()
    val workspaceOnlyDataplaneGroupId = UUID.randomUUID()
    val workspaceOnlyId = UUID.randomUUID()
    val sameOrgOtherWorkspaceId = UUID.randomUUID()
    val workspaceOnlyReservationJobId = UUID.randomUUID().mostSignificantBits
    val sameOrgOtherReservationJobId = UUID.randomUUID().mostSignificantBits
    val workspaceOnlyManifest =
      emptyManifestForUser(userId).copy(
        workspaceIds = listOf(workspaceOnlyId),
        workspaceRefs = listOf(DsrManifest.ManifestWorkspace(workspaceOnlyId, "workspace-only")),
        authUsers = listOf(DsrManifest.ManifestAuthUser("kc-auth-integration", "KEYCLOAK")),
      )

    database!!.query { ctx: DSLContext ->
      insertOrganization(ctx, workspaceOnlyOrganizationId, unrelatedUserId, "workspace-only-org", "workspace-only@airbyte.io")
      insertDataplaneGroup(ctx, workspaceOnlyDataplaneGroupId, workspaceOnlyOrganizationId, "workspace-only-dataplane-group")
      insertWorkspace(ctx, workspaceOnlyId, workspaceOnlyOrganizationId, workspaceOnlyDataplaneGroupId, "workspace-only", email)
      insertWorkspace(
        ctx,
        sameOrgOtherWorkspaceId,
        workspaceOnlyOrganizationId,
        workspaceOnlyDataplaneGroupId,
        "same-org-other-workspace",
        "same-org-other@airbyte.io",
      )
      insertDataWorkerUsage(ctx, workspaceOnlyOrganizationId, workspaceOnlyId, workspaceOnlyDataplaneGroupId)
      insertDataWorkerUsageReservation(
        ctx,
        workspaceOnlyReservationJobId,
        workspaceOnlyOrganizationId,
        workspaceOnlyId,
        workspaceOnlyDataplaneGroupId,
      )
      insertDataWorkerUsage(ctx, workspaceOnlyOrganizationId, sameOrgOtherWorkspaceId, workspaceOnlyDataplaneGroupId)
      insertDataWorkerUsageReservation(
        ctx,
        sameOrgOtherReservationJobId,
        workspaceOnlyOrganizationId,
        sameOrgOtherWorkspaceId,
        workspaceOnlyDataplaneGroupId,
      )
    }

    database!!.transaction { ctx: DSLContext ->
      service.hardDeleteConfigRowsForTest(ctx, workspaceOnlyManifest, datagrailId, emptyList())
    }

    assertDeletedUuidField("data_worker_usage", "workspace_id", workspaceOnlyId)
    assertDeletedLong("data_worker_usage_reservation", "job_id", workspaceOnlyReservationJobId)
    assertPresentUuidField("data_worker_usage", "workspace_id", sameOrgOtherWorkspaceId)
    assertPresentLong("data_worker_usage_reservation", "job_id", sameOrgOtherReservationJobId)
  }

  @Test
  fun `execute fails and skips Keycloak deletion when the config user tombstone update misses the user row`() {
    val requestId = UUID.randomUUID()
    val missingUserId = UUID.randomUUID()
    val missingUserManifest = emptyManifestForUser(missingUserId)
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = missingUserId,
        requestedBy = "support@airbyte.io",
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(missingUserManifest),
      )
    val finalizations = captureActiveFinalization(requestId)

    every { userPersistence.getUserByEmail(email) } returns
      Optional.of(
        User()
          .withUserId(missingUserId)
          .withEmail(email)
          .withName("Davin Integration"),
      )
    every { workspaceRepository.findByEmailIgnoreCase(email) } returns emptyList()
    every { organizationRepository.findByEmailIgnoreCase(email) } returns emptyList()
    every { permissionRepository.findByUserId(missingUserId) } returns emptyList()
    every { userPersistence.listAuthUsersForUser(missingUserId) } returns emptyList()
    every { dbPrune.countJobsAndAttemptsByScopes(emptyList()) } returns DbPrune.JobScopeCounts(jobCount = 0L, attemptCount = 0L)
    every { dbPrune.pruneJobsAndAttemptsByScopes(emptyList()) } returns DbPrune.JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
    every { externalUserService.findUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns emptyList()
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.markRunningIfPreviewed(requestId, email, datagrailId, oncallIssueNumber, executedBy, any())
    } answers {
      row.status = DataSubjectDeletionStatus.running
      row.confirmedBy = executedBy
      1
    }

    val startExecutionResult = service.startExecution(requestId, email, datagrailId, oncallIssueNumber, executedBy)
    val executeRequest = service.executeClaimedRequest(startExecutionResult.requestId)

    assertEquals("FAILED", executeRequest.status)
    assertFalse(executeRequest.tombstonedUser)
    assertTrue(executeRequest.errors.single().contains("Expected to tombstone exactly one Airbyte user row"))
    assertEquals(DataSubjectDeletionStatus.failed, finalizations.single().finalStatus)
    assertEquals(0, objectMapper.readTree(finalizations.single().executionCounts).get("deleted_keycloak_user_count").asInt())
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  private fun seedConfigDatabase() {
    val now = OffsetDateTime.now(ZoneOffset.UTC)
    database!!.query { ctx: DSLContext ->
      insertUser(ctx, userId, "Davin Integration", email)
      insertUser(ctx, coMemberUserId, "Co Member Integration", "co.member.integration@airbyte.io")
      insertUser(ctx, unrelatedUserId, "Unaffected User", "unaffected.integration@airbyte.io")

      insertOrganization(ctx, organizationId, userId, "dsr-org", email)
      insertOrganization(ctx, unrelatedOrganizationId, unrelatedUserId, "unrelated-org", "unaffected.integration@airbyte.io")

      insertDataplaneGroup(ctx, dataplaneGroupId, organizationId, "dsr-dataplane-group")
      insertDataplaneGroup(ctx, unrelatedDataplaneGroupId, unrelatedOrganizationId, "unrelated-dataplane-group")

      insertWorkspace(ctx, workspaceId, organizationId, dataplaneGroupId, "dsr-workspace", email)
      insertWorkspace(
        ctx = ctx,
        id = unrelatedWorkspaceId,
        organizationId = unrelatedOrganizationId,
        dataplaneGroupId = unrelatedDataplaneGroupId,
        name = "unrelated-workspace",
        email = "unaffected.integration@airbyte.io",
      )

      ctx
        .update(ConfigTables.USER)
        .set(ConfigTables.USER.DEFAULT_WORKSPACE_ID, workspaceId)
        .where(ConfigTables.USER.ID.`in`(userId, coMemberUserId))
        .execute()

      insertActorDefinition(ctx, sourceDefinitionId, "source-definition", ActorType.source)
      insertActorDefinition(ctx, destinationDefinitionId, "destination-definition", ActorType.destination)
      insertActor(ctx, sourceId, workspaceId, sourceDefinitionId, "postgres integration", ActorType.source)
      insertActor(ctx, destinationId, workspaceId, destinationDefinitionId, "bigquery integration", ActorType.destination)
      insertActor(ctx, unrelatedActorId, unrelatedWorkspaceId, sourceDefinitionId, "unrelated source", ActorType.source)

      ctx
        .insertInto(ConfigTables.ACTOR_CATALOG)
        .set(ConfigTables.ACTOR_CATALOG.ID, actorCatalogId)
        .set(ConfigTables.ACTOR_CATALOG.CATALOG, JSONB.valueOf("{}"))
        .set(ConfigTables.ACTOR_CATALOG.CATALOG_HASH, "12345678901234567890123456789012")
        .set(ConfigTables.ACTOR_CATALOG.CREATED_AT, now)
        .set(ConfigTables.ACTOR_CATALOG.CATALOG_TYPE, ActorCatalogType.source_catalog)
        .execute()
      ctx
        .insertInto(ConfigTables.ACTOR_CATALOG_FETCH_EVENT)
        .set(ConfigTables.ACTOR_CATALOG_FETCH_EVENT.ID, actorCatalogFetchEventId)
        .set(ConfigTables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_CATALOG_ID, actorCatalogId)
        .set(ConfigTables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_ID, sourceId)
        .set(ConfigTables.ACTOR_CATALOG_FETCH_EVENT.CONFIG_HASH, "abcdef78901234567890123456789012")
        .set(ConfigTables.ACTOR_CATALOG_FETCH_EVENT.ACTOR_VERSION, "1.0.0")
        .execute()

      ctx
        .insertInto(ConfigTables.CONNECTION)
        .set(ConfigTables.CONNECTION.ID, connectionId)
        .set(ConfigTables.CONNECTION.NAMESPACE_DEFINITION, NamespaceDefinitionType.source)
        .set(ConfigTables.CONNECTION.SOURCE_ID, sourceId)
        .set(ConfigTables.CONNECTION.DESTINATION_ID, destinationId)
        .set(ConfigTables.CONNECTION.NAME, "integration connection")
        .set(ConfigTables.CONNECTION.CATALOG, JSONB.valueOf("{}"))
        .set(ConfigTables.CONNECTION.STATUS, StatusType.active)
        .set(ConfigTables.CONNECTION.SOURCE_CATALOG_ID, actorCatalogId)
        .execute()
      ctx
        .insertInto(ConfigTables.STATE)
        .set(ConfigTables.STATE.ID, stateId)
        .set(ConfigTables.STATE.CONNECTION_ID, connectionId)
        .set(ConfigTables.STATE.STATE_, JSONB.valueOf("{}"))
        .execute()

      ctx
        .insertInto(ConfigTables.CONNECTOR_BUILDER_PROJECT)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.ID, builderProjectId)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.NAME, "builder project")
        .execute()
      ctx
        .insertInto(ConfigTables.CONNECTOR_BUILDER_PROJECT)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.ID, tombstonedBuilderProjectId)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.NAME, "tombstoned builder project")
        .set(ConfigTables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE, true)
        .execute()
      ctx
        .insertInto(ConfigTables.PERMISSION)
        .set(ConfigTables.PERMISSION.ID, permissionId)
        .set(ConfigTables.PERMISSION.USER_ID, userId)
        .set(ConfigTables.PERMISSION.WORKSPACE_ID, workspaceId)
        .set(ConfigTables.PERMISSION.PERMISSION_TYPE, PermissionType.workspace_admin)
        .execute()
      ctx
        .insertInto(ConfigTables.AUTH_USER)
        .set(ConfigTables.AUTH_USER.ID, authUserRowId)
        .set(ConfigTables.AUTH_USER.USER_ID, userId)
        .set(ConfigTables.AUTH_USER.AUTH_USER_ID, "kc-auth-integration")
        .set(ConfigTables.AUTH_USER.AUTH_PROVIDER, JooqAuthProvider.keycloak)
        .execute()
      ctx
        .insertInto(ConfigTables.APPLICATION)
        .set(ConfigTables.APPLICATION.ID, applicationId)
        .set(ConfigTables.APPLICATION.AUTH_USER_ID, "kc-auth-integration")
        .set(ConfigTables.APPLICATION.NAME, "dsr application")
        .set(ConfigTables.APPLICATION.CLIENT_ID, "dsr-client-id")
        .set(ConfigTables.APPLICATION.CLIENT_SECRET, "dsr-client-secret")
        .execute()
      ctx
        .insertInto(ConfigTables.APPLICATION)
        .set(ConfigTables.APPLICATION.ID, unrelatedApplicationId)
        .set(ConfigTables.APPLICATION.AUTH_USER_ID, "unrelated-auth-user")
        .set(ConfigTables.APPLICATION.NAME, "unrelated application")
        .set(ConfigTables.APPLICATION.CLIENT_ID, "unrelated-client-id")
        .set(ConfigTables.APPLICATION.CLIENT_SECRET, "unrelated-client-secret")
        .execute()
      ctx
        .insertInto(ConfigTables.SCOPED_CONFIGURATION)
        .set(ConfigTables.SCOPED_CONFIGURATION.ID, scopedConfigurationId)
        .set(ConfigTables.SCOPED_CONFIGURATION.KEY, "dsr.connection.setting")
        .set(ConfigTables.SCOPED_CONFIGURATION.RESOURCE_TYPE, ConfigResourceType.connection)
        .set(ConfigTables.SCOPED_CONFIGURATION.RESOURCE_ID, connectionId)
        .set(ConfigTables.SCOPED_CONFIGURATION.SCOPE_TYPE, ConfigScopeType.workspace)
        .set(ConfigTables.SCOPED_CONFIGURATION.SCOPE_ID, workspaceId)
        .set(ConfigTables.SCOPED_CONFIGURATION.VALUE, "enabled")
        .set(ConfigTables.SCOPED_CONFIGURATION.ORIGIN_TYPE, ConfigOriginType.user)
        .set(ConfigTables.SCOPED_CONFIGURATION.ORIGIN, "integration-test")
        .execute()
      ctx
        .insertInto(ConfigTables.SCOPED_CONFIGURATION)
        .set(ConfigTables.SCOPED_CONFIGURATION.ID, unrelatedScopedConfigurationId)
        .set(ConfigTables.SCOPED_CONFIGURATION.KEY, "unrelated.setting")
        .set(ConfigTables.SCOPED_CONFIGURATION.RESOURCE_TYPE, ConfigResourceType.source)
        .set(ConfigTables.SCOPED_CONFIGURATION.RESOURCE_ID, unrelatedActorId)
        .set(ConfigTables.SCOPED_CONFIGURATION.SCOPE_TYPE, ConfigScopeType.workspace)
        .set(ConfigTables.SCOPED_CONFIGURATION.SCOPE_ID, unrelatedWorkspaceId)
        .set(ConfigTables.SCOPED_CONFIGURATION.VALUE, "enabled")
        .set(ConfigTables.SCOPED_CONFIGURATION.ORIGIN_TYPE, ConfigOriginType.user)
        .set(ConfigTables.SCOPED_CONFIGURATION.ORIGIN, "integration-test")
        .execute()

      insertObservabilityJobStats(ctx, observabilityJobId, connectionId, workspaceId, organizationId)
      insertObservabilityStreamStats(ctx, observabilityJobId)
      insertDataWorkerUsage(ctx, organizationId, workspaceId, dataplaneGroupId)
      insertDataWorkerUsageReservation(ctx, dataWorkerUsageReservationJobId, organizationId, workspaceId, dataplaneGroupId)

      insertObservabilityJobStats(ctx, unrelatedObservabilityJobId, UUID.randomUUID(), unrelatedWorkspaceId, unrelatedOrganizationId)
      insertObservabilityStreamStats(ctx, unrelatedObservabilityJobId)
      insertDataWorkerUsage(ctx, unrelatedOrganizationId, unrelatedWorkspaceId, unrelatedDataplaneGroupId)
      insertDataWorkerUsageReservation(
        ctx,
        unrelatedDataWorkerUsageReservationJobId,
        unrelatedOrganizationId,
        unrelatedWorkspaceId,
        unrelatedDataplaneGroupId,
      )

      insertWorkload(ctx, legacySyncWorkloadId, workspaceId, organizationId, "sync")
      insertWorkloadQueue(ctx, legacySyncWorkloadId, dataplaneGroupId)
      insertCommand(ctx, commandId, legacySyncWorkloadId, workspaceId, organizationId)
      insertWorkload(ctx, orphanedSyncWorkloadId, workspaceId, organizationId, "sync")
      insertWorkloadQueue(ctx, orphanedSyncWorkloadId, dataplaneGroupId)
      insertWorkload(ctx, actorCheckWorkloadId, workspaceId, organizationId, "check")
      insertWorkloadQueue(ctx, actorCheckWorkloadId, dataplaneGroupId)
      insertWorkload(ctx, actorDefinitionDiscoverWorkloadId, workspaceId, organizationId, "discover")
      insertWorkloadQueue(ctx, actorDefinitionDiscoverWorkloadId, dataplaneGroupId)

      insertWorkload(ctx, unrelatedWorkloadId, unrelatedWorkspaceId, unrelatedOrganizationId, "sync")
      insertWorkload(ctx, unrelatedActorDefinitionDiscoverWorkloadId, unrelatedWorkspaceId, unrelatedOrganizationId, "discover")
      insertWorkloadQueue(ctx, unrelatedActorDefinitionDiscoverWorkloadId, unrelatedDataplaneGroupId)
      insertCommand(ctx, unrelatedCommandId, unrelatedWorkloadId, unrelatedWorkspaceId, unrelatedOrganizationId)
    }
  }

  private fun stubManifestDependencies() {
    every { userPersistence.getUserByEmail(email) } returns
      Optional.of(
        User()
          .withUserId(userId)
          .withEmail(email)
          .withName("Davin Integration"),
      )
    every { workspaceRepository.findByEmailIgnoreCase(email) } returns
      listOf(
        Workspace(
          id = workspaceId,
          name = "dsr-workspace",
          slug = "dsr-workspace",
          email = email,
          dataplaneGroupId = dataplaneGroupId,
          organizationId = organizationId,
        ),
      )
    every { organizationRepository.findByEmailIgnoreCase(email) } returns
      listOf(Organization(id = organizationId, name = "dsr-org", userId = userId, email = email))
    every { connectionService.listConnectionIdsForWorkspace(workspaceId) } returns listOf(connectionId)
    every { connectionService.getStandardSync(connectionId) } returns
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
    every { sourceService.listWorkspaceSourceConnection(workspaceId) } returns
      listOf(SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres integration"))
    every { sourceService.listWorkspacesSourceConnections(any()) } answers {
      val query = firstArg<ResourcesQueryPaginated>()
      if (workspaceId in query.workspaceIds) {
        listOf(SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres integration"))
      } else {
        emptyList()
      }
    }
    every { sourceService.getSourceConnection(sourceId) } returns
      SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres integration")
    every { destinationService.listWorkspaceDestinationConnection(workspaceId) } returns
      listOf(DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery integration"))
    every { destinationService.listWorkspacesDestinationConnections(any()) } answers {
      val query = firstArg<ResourcesQueryPaginated>()
      if (workspaceId in query.workspaceIds) {
        listOf(DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery integration"))
      } else {
        emptyList()
      }
    }
    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery integration")
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } returns
      Stream.of(
        ConnectorBuilderProject()
          .withBuilderProjectId(builderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("builder project"),
      )
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId, true) } returns
      Stream.of(
        ConnectorBuilderProject()
          .withBuilderProjectId(builderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("builder project"),
        ConnectorBuilderProject()
          .withBuilderProjectId(tombstonedBuilderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("tombstoned builder project")
          .withTombstone(true),
      )
    every { permissionRepository.findByUserId(userId) } returns
      listOf(
        Permission(
          id = permissionId,
          userId = userId,
          workspaceId = workspaceId,
          permissionType = PermissionType.workspace_admin,
        ),
      )
    every { userPersistence.listAuthUsersForUser(userId) } returns
      listOf(
        AuthUser()
          .withUserId(userId)
          .withAuthUserId("kc-auth-integration")
          .withAuthProvider(ConfigAuthProvider.KEYCLOAK),
      )
    every { dbPrune.countJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobScopeCounts(jobCount = 3L, attemptCount = 4L)
    every { externalUserService.findUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns
      listOf(ExternalUserService.ExternalUser("kc-auth-integration", email, "davin.integration", true))
    every { connectionManagerUtils.isWorkflowStateRunning(connectionId) } returns false
  }

  private fun insertUser(
    ctx: DSLContext,
    id: UUID,
    name: String,
    email: String,
  ) {
    ctx
      .insertInto(ConfigTables.USER)
      .set(ConfigTables.USER.ID, id)
      .set(ConfigTables.USER.NAME, name)
      .set(ConfigTables.USER.EMAIL, email)
      .set(ConfigTables.USER.STATUS, Status.registered)
      .execute()
  }

  private fun insertOrganization(
    ctx: DSLContext,
    id: UUID,
    userId: UUID,
    name: String,
    email: String,
  ) {
    ctx
      .insertInto(ConfigTables.ORGANIZATION)
      .set(ConfigTables.ORGANIZATION.ID, id)
      .set(ConfigTables.ORGANIZATION.NAME, name)
      .set(ConfigTables.ORGANIZATION.USER_ID, userId)
      .set(ConfigTables.ORGANIZATION.EMAIL, email)
      .execute()
  }

  private fun insertDataplaneGroup(
    ctx: DSLContext,
    id: UUID,
    organizationId: UUID,
    name: String,
  ) {
    ctx
      .insertInto(ConfigTables.DATAPLANE_GROUP)
      .set(ConfigTables.DATAPLANE_GROUP.ID, id)
      .set(ConfigTables.DATAPLANE_GROUP.ORGANIZATION_ID, organizationId)
      .set(ConfigTables.DATAPLANE_GROUP.NAME, name)
      .execute()
  }

  private fun insertWorkspace(
    ctx: DSLContext,
    id: UUID,
    organizationId: UUID,
    dataplaneGroupId: UUID,
    name: String,
    email: String,
  ) {
    ctx
      .insertInto(ConfigTables.WORKSPACE)
      .set(ConfigTables.WORKSPACE.ID, id)
      .set(ConfigTables.WORKSPACE.NAME, name)
      .set(ConfigTables.WORKSPACE.SLUG, name)
      .set(ConfigTables.WORKSPACE.EMAIL, email)
      .set(ConfigTables.WORKSPACE.INITIAL_SETUP_COMPLETE, true)
      .set(ConfigTables.WORKSPACE.ORGANIZATION_ID, organizationId)
      .set(ConfigTables.WORKSPACE.DATAPLANE_GROUP_ID, dataplaneGroupId)
      .execute()
  }

  private fun insertActorDefinition(
    ctx: DSLContext,
    id: UUID,
    name: String,
    actorType: ActorType,
  ) {
    ctx
      .insertInto(ConfigTables.ACTOR_DEFINITION)
      .set(ConfigTables.ACTOR_DEFINITION.ID, id)
      .set(ConfigTables.ACTOR_DEFINITION.NAME, name)
      .set(ConfigTables.ACTOR_DEFINITION.ACTOR_TYPE, actorType)
      .execute()
  }

  private fun insertActor(
    ctx: DSLContext,
    id: UUID,
    workspaceId: UUID,
    actorDefinitionId: UUID,
    name: String,
    actorType: ActorType,
  ) {
    ctx
      .insertInto(ConfigTables.ACTOR)
      .set(ConfigTables.ACTOR.ID, id)
      .set(ConfigTables.ACTOR.WORKSPACE_ID, workspaceId)
      .set(ConfigTables.ACTOR.ACTOR_DEFINITION_ID, actorDefinitionId)
      .set(ConfigTables.ACTOR.NAME, name)
      .set(ConfigTables.ACTOR.CONFIGURATION, JSONB.valueOf("{}"))
      .set(ConfigTables.ACTOR.ACTOR_TYPE, actorType)
      .execute()
  }

  private fun insertObservabilityJobStats(
    ctx: DSLContext,
    jobId: Long,
    connectionId: UUID,
    workspaceId: UUID,
    organizationId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO observability_jobs_stats (
        job_id,
        connection_id,
        workspace_id,
        organization_id,
        source_id,
        source_definition_id,
        destination_id,
        destination_definition_id,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      """.trimIndent(),
      jobId,
      connectionId,
      workspaceId,
      organizationId,
      sourceId,
      sourceDefinitionId,
      destinationId,
      destinationDefinitionId,
    )
  }

  private fun insertObservabilityStreamStats(
    ctx: DSLContext,
    jobId: Long,
  ) {
    ctx.execute(
      """
      INSERT INTO observability_stream_stats (
        job_id,
        stream_namespace,
        stream_name,
        bytes_loaded,
        records_loaded,
        records_rejected,
        was_backfilled,
        was_resumed
      )
      VALUES (?, NULL, ?, 0, 0, 0, FALSE, FALSE)
      """.trimIndent(),
      jobId,
      "users_$jobId",
    )
  }

  private fun insertDataWorkerUsage(
    ctx: DSLContext,
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO data_worker_usage (
        organization_id,
        workspace_id,
        dataplane_group_id,
        bucket_start,
        source_cpu_request,
        destination_cpu_request,
        orchestrator_cpu_request
      )
      VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1.0, 1.0, 1.0)
      """.trimIndent(),
      organizationId,
      workspaceId,
      dataplaneGroupId,
    )
  }

  private fun insertDataWorkerUsageReservation(
    ctx: DSLContext,
    jobId: Long,
    organizationId: UUID,
    workspaceId: UUID,
    dataplaneGroupId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO data_worker_usage_reservation (
        job_id,
        organization_id,
        workspace_id,
        dataplane_group_id,
        source_cpu_request,
        destination_cpu_request,
        orchestrator_cpu_request
      )
      VALUES (?, ?, ?, ?, 1.0, 1.0, 1.0)
      """.trimIndent(),
      jobId,
      organizationId,
      workspaceId,
      dataplaneGroupId,
    )
  }

  private fun insertWorkload(
    ctx: DSLContext,
    id: String,
    workspaceId: UUID,
    organizationId: UUID,
    type: String,
  ) {
    ctx.execute(
      """
      INSERT INTO workload (id, status, input_payload, log_path, type, workspace_id, organization_id)
      VALUES (?, 'success'::workload_status, '{}', ?, ?::workload_type, ?, ?)
      """.trimIndent(),
      id,
      "/logs/$id",
      type,
      workspaceId,
      organizationId,
    )
  }

  private fun insertWorkloadQueue(
    ctx: DSLContext,
    workloadId: String,
    dataplaneGroupId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO workload_queue (id, dataplane_group, priority, workload_id)
      VALUES (?, ?, 0, ?)
      """.trimIndent(),
      UUID.randomUUID(),
      dataplaneGroupId.toString(),
      workloadId,
    )
  }

  private fun insertCommand(
    ctx: DSLContext,
    id: String,
    workloadId: String,
    workspaceId: UUID,
    organizationId: UUID,
  ) {
    ctx.execute(
      """
      INSERT INTO commands (id, workload_id, command_type, command_input, workspace_id, organization_id)
      VALUES (?, ?, 'SPEC', '{}'::jsonb, ?, ?)
      """.trimIndent(),
      id,
      workloadId,
      workspaceId,
      organizationId,
    )
  }

  private fun assertDeleted(
    tableName: String,
    id: UUID,
  ) {
    assertEquals(0, countByUuidField(tableName, "id", id), "$tableName row $id should be deleted")
  }

  private fun assertPresent(
    tableName: String,
    id: UUID,
  ) {
    assertEquals(1, countByUuidField(tableName, "id", id), "$tableName row $id should be present")
  }

  private fun assertDeletedUuidField(
    tableName: String,
    fieldName: String,
    id: UUID,
  ) {
    assertEquals(0, countByUuidField(tableName, fieldName, id), "$tableName row with $fieldName=$id should be deleted")
  }

  private fun assertPresentUuidField(
    tableName: String,
    fieldName: String,
    id: UUID,
  ) {
    assertEquals(1, countByUuidField(tableName, fieldName, id), "$tableName row with $fieldName=$id should be present")
  }

  private fun assertDeletedLong(
    tableName: String,
    fieldName: String,
    value: Long,
  ) {
    assertEquals(0, countByLongField(tableName, fieldName, value), "$tableName row $value should be deleted")
  }

  private fun assertPresentLong(
    tableName: String,
    fieldName: String,
    value: Long,
  ) {
    assertEquals(1, countByLongField(tableName, fieldName, value), "$tableName row $value should be present")
  }

  private fun assertDeletedString(
    tableName: String,
    fieldName: String,
    value: String,
  ) {
    assertEquals(0, countByStringField(tableName, fieldName, value), "$tableName row $value should be deleted")
  }

  private fun assertPresentString(
    tableName: String,
    fieldName: String,
    value: String,
  ) {
    assertEquals(1, countByStringField(tableName, fieldName, value), "$tableName row $value should be present")
  }

  private fun countByUuidField(
    tableName: String,
    fieldName: String,
    id: UUID,
  ): Int =
    database!!.query { ctx: DSLContext ->
      ctx.fetchCount(
        DSL.table(DSL.name("public", tableName)),
        DSL.field(DSL.name("public", tableName, fieldName), UUID::class.java).eq(id),
      )
    }

  private fun countByLongField(
    tableName: String,
    fieldName: String,
    value: Long,
  ): Int =
    database!!.query { ctx: DSLContext ->
      ctx.fetchCount(
        DSL.table(DSL.name("public", tableName)),
        DSL.field(DSL.name("public", tableName, fieldName), Long::class.java).eq(value),
      )
    }

  private fun countByStringField(
    tableName: String,
    fieldName: String,
    value: String,
  ): Int =
    database!!.query { ctx: DSLContext ->
      ctx.fetchCount(
        DSL.table(DSL.name("public", tableName)),
        DSL.field(DSL.name("public", tableName, fieldName), String::class.java).eq(value),
      )
    }

  private fun manifest(): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin Integration"),
      workspaceIds = listOf(workspaceId),
      workspaceRefs = listOf(DsrManifest.ManifestWorkspace(workspaceId, "dsr-workspace")),
      organizationIds = listOf(organizationId),
      organizationRefs = listOf(DsrManifest.ManifestOrganization(organizationId, "dsr-org")),
      connectionIds = listOf(connectionId),
      connectionRefs =
        listOf(
          DsrManifest.ManifestConnection(
            connectionId = connectionId,
            sourceId = sourceId,
            sourceName = "postgres integration",
            destinationId = destinationId,
            destinationName = "bigquery integration",
          ),
        ),
      sourceIds = listOf(sourceId),
      destinationIds = listOf(destinationId),
      connectorBuilderProjectIds = listOf(builderProjectId, tombstonedBuilderProjectId),
      permissionIds = listOf(permissionId),
      authUsers = listOf(DsrManifest.ManifestAuthUser("kc-auth-integration", "KEYCLOAK")),
      jobCount = 3L,
      attemptCount = 4L,
      keycloakUsers = listOf(DsrManifest.ManifestKeycloakUser("kc-auth-integration", email, "davin.integration", true)),
      temporalWorkflows = emptyList(),
    )

  private fun emptyManifestForUser(userId: UUID): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin Integration"),
      workspaceIds = emptyList(),
      workspaceRefs = emptyList(),
      organizationIds = emptyList(),
      organizationRefs = emptyList(),
      connectionIds = emptyList(),
      connectionRefs = emptyList(),
      sourceIds = emptyList(),
      destinationIds = emptyList(),
      connectorBuilderProjectIds = emptyList(),
      permissionIds = emptyList(),
      authUsers = emptyList(),
      jobCount = 0L,
      attemptCount = 0L,
      keycloakUsers = emptyList(),
      temporalWorkflows = emptyList(),
    )
}
