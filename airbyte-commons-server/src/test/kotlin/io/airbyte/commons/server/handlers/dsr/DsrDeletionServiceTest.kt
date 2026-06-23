/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.dsr

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.temporal.ConnectionManagerUtils
import io.airbyte.config.AuthProvider
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
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.Database
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.airbyte.domain.services.dsr.DsrDeletionRequestTimeoutService
import io.airbyte.domain.services.dsr.DsrManifest
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.DbPrune
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.mockk.verifyOrder
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.lang.reflect.InvocationTargetException
import java.time.Duration
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

internal class DsrDeletionServiceTest {
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
  private lateinit var configDatabase: Database
  private lateinit var metricClient: MetricClient
  private lateinit var objectMapper: ObjectMapper
  private lateinit var executionHeartbeatExecutor: ScheduledExecutorService
  private lateinit var executionHeartbeatFuture: ScheduledFuture<*>
  private lateinit var service: DsrDeletionService
  private var configDeletionCounts = DsrDeletionService.ConfigDeletionCounts.empty()

  private val email = "davin+2@airbyte.io"
  private val datagrailId = "dg-abc-123"
  private val oncallIssueNumber = "ONCALL-1234"
  private val requestedBy = "support@airbyte.io"
  private val emailHash = DsrDeletionService.emailHash(email)
  private val userId: UUID = UUID.randomUUID()
  private val workspaceId: UUID = UUID.randomUUID()
  private val orgId: UUID = UUID.randomUUID()
  private val connectionId: UUID = UUID.randomUUID()
  private val sourceId: UUID = UUID.randomUUID()
  private val destinationId: UUID = UUID.randomUUID()
  private val builderProjectId: UUID = UUID.randomUUID()
  private val permissionId: UUID = UUID.randomUUID()

  private data class FinalizedExecution(
    val finalStatus: DataSubjectDeletionStatus,
    val scrubbedEmail: String,
    val scrubbedManifest: String,
    val confirmErrors: String?,
    val executionCounts: String,
  )

  @BeforeEach
  fun setup() {
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
    configDeletionCounts = DsrDeletionService.ConfigDeletionCounts.empty()

    configDatabase = mockk()
    val dslContext = DSL.using(org.jooq.SQLDialect.POSTGRES)
    every { configDatabase.query<Any>(any<ContextQueryFunction<Any>>()) } answers {
      val fn = firstArg<ContextQueryFunction<Any>>()
      fn.query(dslContext)
    }
    every { configDatabase.transaction<DsrDeletionService.ConfigDeletionCounts>(any()) } answers {
      val fn = firstArg<ContextQueryFunction<DsrDeletionService.ConfigDeletionCounts>>()
      fn.query(dslContext)
    }

    every { userPersistence.getUserByEmail(email) } returns
      Optional.of(
        User().apply {
          userId = this@DsrDeletionServiceTest.userId
          email = this@DsrDeletionServiceTest.email
          name = "Davin"
        },
      )
    every { workspaceRepository.findByEmailIgnoreCase(email) } returns
      listOf(
        Workspace(
          id = workspaceId,
          name = "my-workspace",
          slug = "my-workspace",
          email = email,
          dataplaneGroupId = UUID.randomUUID(),
          organizationId = orgId,
        ),
      )
    every { organizationRepository.findByEmailIgnoreCase(email) } returns
      listOf(Organization(id = orgId, name = "my-org", userId = userId, email = email))
    every { connectionService.listConnectionIdsForWorkspace(workspaceId) } returns listOf(connectionId)
    every { connectionService.getStandardSync(connectionId) } returns
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(sourceId)
        .withDestinationId(destinationId)
    every { sourceService.listWorkspaceSourceConnection(workspaceId) } returns
      listOf(SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres"))
    every { sourceService.listWorkspacesSourceConnections(any()) } answers {
      val query = firstArg<ResourcesQueryPaginated>()
      if (workspaceId in query.workspaceIds) {
        listOf(SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres"))
      } else {
        emptyList()
      }
    }
    every { sourceService.getSourceConnection(sourceId) } returns
      SourceConnection().withSourceId(sourceId).withWorkspaceId(workspaceId).withName("postgres")
    every { destinationService.listWorkspaceDestinationConnection(workspaceId) } returns
      listOf(DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery"))
    every { destinationService.listWorkspacesDestinationConnections(any()) } answers {
      val query = firstArg<ResourcesQueryPaginated>()
      if (workspaceId in query.workspaceIds) {
        listOf(DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery"))
      } else {
        emptyList()
      }
    }
    every { destinationService.getDestinationConnection(destinationId) } returns
      DestinationConnection().withDestinationId(destinationId).withWorkspaceId(workspaceId).withName("bigquery")
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } answers {
      java.util.stream.Stream
        .empty()
    }
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId, true) } answers {
      java.util.stream.Stream
        .empty()
    }
    every { permissionRepository.findByUserId(userId) } returns
      listOf(
        Permission(
          id = permissionId,
          userId = userId,
          workspaceId = workspaceId,
          permissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin,
        ),
      )
    every { userPersistence.listAuthUsersForUser(userId) } returns
      listOf(AuthUser().withUserId(userId).withAuthUserId("kc-auth-1").withAuthProvider(AuthProvider.KEYCLOAK))
    every { externalUserService.findUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns
      listOf(ExternalUserService.ExternalUser("kc-auth-1", email, "Davin", true))
    every { connectionManagerUtils.isWorkflowStateRunning(connectionId) } returns true
    every { connectionManagerUtils.getConnectionManagerName(connectionId) } returns "connection_manager_$connectionId"
    every { dbPrune.countJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobScopeCounts(jobCount = 7L, attemptCount = 9L)
    every { deletionRequestRepository.findActiveByEmailHash(emailHash) } returns Optional.empty()
    every { deletionRequestRepository.save(any<DataSubjectDeletionRequest>()) } answers {
      val arg = firstArg<DataSubjectDeletionRequest>()
      arg.id = arg.id ?: UUID.randomUUID()
      arg
    }
    every { deletionRequestRepository.markRunningExecutionStarted(any(), any()) } returns 1
    every { deletionRequestRepository.heartbeatRunningExecution(any(), any()) } returns 1
    every { deletionRequestRepository.finalizeRunningExecutionIfActive(any(), any(), any(), any(), any(), any(), any()) } returns 1
    every { deletionRequestRepository.failRunningExecutionIfRunning(any(), any(), any(), any(), any(), any()) } returns 1
    every {
      executionHeartbeatExecutor.scheduleAtFixedRate(any(), any(), any(), TimeUnit.MILLISECONDS)
    } returns executionHeartbeatFuture
    every { executionHeartbeatFuture.cancel(false) } returns true

    service = buildService()
  }

  private fun buildService(): DsrDeletionService =
    object : DsrDeletionService(
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
      configDatabase = configDatabase,
      executionTimeout = Duration.ofMinutes(30),
      executionHeartbeatExecutor = executionHeartbeatExecutor,
      executionHeartbeatInterval = Duration.ofMinutes(1),
    ) {
      override fun hardDeleteConfigRows(
        ctx: DSLContext,
        manifest: DsrManifest,
        datagrailId: String,
        syncWorkloadIds: List<String>,
      ): DsrDeletionService.ConfigDeletionCounts = configDeletionCounts
    }

  private fun captureActiveFinalization(
    requestId: UUID,
    updatedRows: Int = 1,
  ): MutableList<FinalizedExecution> {
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
      updatedRows
    }
    return finalizations
  }

  private fun captureUnexpectedFailureFinalization(requestId: UUID): MutableList<FinalizedExecution> {
    val finalizations = mutableListOf<FinalizedExecution>()
    every {
      deletionRequestRepository.failRunningExecutionIfRunning(requestId, any(), any(), any(), any(), any())
    } answers {
      val args = invocation.args
      finalizations.add(
        FinalizedExecution(
          finalStatus = DataSubjectDeletionStatus.failed,
          scrubbedEmail = args[2] as String,
          scrubbedManifest = args[3] as String,
          confirmErrors = args[4] as String?,
          executionCounts = args[5] as String,
        ),
      )
      1
    }
    return finalizations
  }

  @Test
  fun `workload prefix range condition renders C collation`() {
    val method =
      DsrDeletionService::class.java.getDeclaredMethod(
        "prefixRangeCondition",
        org.jooq.Field::class.java,
        String::class.java,
      )
    method.isAccessible = true

    val condition =
      method.invoke(
        service,
        DSL.field(DSL.name("public", "workload", "id"), String::class.java),
        "${connectionId}_",
      ) as org.jooq.Condition
    val sql = DSL.using(org.jooq.SQLDialect.POSTGRES).renderInlined(condition)

    assertTrue(Regex("(?i)collate\\s+\"C\"").findAll(sql).count() >= 4, sql)
  }

  @Test
  fun `workload prefix fallback skips lookup when scope is empty`() {
    val method =
      DsrDeletionService::class.java.getDeclaredMethod(
        "selectDsrWorkloadIds",
        DSLContext::class.java,
        List::class.java,
        List::class.java,
        List::class.java,
        List::class.java,
        List::class.java,
      )
    method.isAccessible = true

    val workloadIds =
      method.invoke(
        service,
        DSL.using(org.jooq.SQLDialect.POSTGRES),
        listOf(connectionId),
        listOf(sourceId),
        listOf(UUID.randomUUID()),
        emptyList<UUID>(),
        emptyList<UUID>(),
      ) as List<*>

    assertTrue(workloadIds.isEmpty())
  }

  @Test
  fun `workload prefix guard rejects ids outside expected prefixes`() {
    val method =
      DsrDeletionService::class.java.getDeclaredMethod(
        "validateDsrWorkloadIdsMatchExpectedPrefixes",
        List::class.java,
        List::class.java,
      )
    method.isAccessible = true

    val unexpectedWorkloadId = "${UUID.randomUUID()}_123_0_sync"
    val exception =
      assertThrows<InvocationTargetException> {
        method.invoke(
          service,
          listOf("${connectionId}_123_0_sync", unexpectedWorkloadId),
          listOf("${connectionId}_", "${sourceId}_"),
        )
      }

    assertTrue(exception.cause is IllegalStateException)
    assertTrue(exception.cause!!.message!!.contains(unexpectedWorkloadId))
  }

  @Test
  fun `preview captures a detailed manifest, persists it, and does not mutate external systems`() {
    val savedSlot = slot<DataSubjectDeletionRequest>()
    every { deletionRequestRepository.save(capture(savedSlot)) } answers {
      savedSlot.captured.id = savedSlot.captured.id ?: UUID.randomUUID()
      savedSlot.captured
    }

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)

    assertEquals("PREVIEWED", result.status)
    assertNotNull(result.requestId)
    assertEquals(email, result.manifest.targetEmail)
    assertEquals(datagrailId, result.manifest.datagrailId)
    assertEquals(userId, result.manifest.userId)
    assertEquals(listOf(DsrManifest.ManifestWorkspace(workspaceId, "my-workspace")), result.manifest.workspaceRefs)
    assertEquals(listOf(DsrManifest.ManifestOrganization(orgId, "my-org")), result.manifest.organizationRefs)
    assertEquals(
      listOf(DsrManifest.ManifestConnection(connectionId, sourceId, "postgres", destinationId, "bigquery")),
      result.manifest.connectionRefs,
    )
    assertEquals(listOf(permissionId), result.manifest.permissionIds)
    assertEquals(7L, result.manifest.jobCount)
    assertEquals(9L, result.manifest.attemptCount)
    assertEquals(listOf(DsrManifest.ManifestKeycloakUser("kc-auth-1", email, "Davin", true)), result.manifest.keycloakUsers)
    assertEquals(
      listOf(DsrManifest.ManifestTemporalWorkflow("connection_manager_$connectionId", connectionId, true)),
      result.manifest.temporalWorkflows,
    )

    val persistedManifest: DsrManifest = objectMapper.readValue(savedSlot.captured.manifest)
    assertEquals(result.manifest, persistedManifest)
    assertEquals(DataSubjectDeletionStatus.previewed, savedSlot.captured.status)
    assertEquals(emailHash, savedSlot.captured.emailHash)
    assertEquals(oncallIssueNumber, savedSlot.captured.oncallIssueNumber)

    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { sourceHandler.deleteSource(any<SourceIdRequestBody>()) }
    verify(exactly = 0) { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) }
    verify(exactly = 0) { dbPrune.pruneJobsByScopes(any()) }
  }

  @Test
  fun `preview includes tombstoned source and destination actors from deleted workspaces`() {
    val tombstonedSourceId = UUID.randomUUID()
    val tombstonedDestinationId = UUID.randomUUID()
    every { sourceService.listWorkspaceSourceConnection(workspaceId) } returns emptyList()
    every { destinationService.listWorkspaceDestinationConnection(workspaceId) } returns emptyList()
    every {
      sourceService.listWorkspacesSourceConnections(match { it.workspaceIds == listOf(workspaceId) && it.includeDeleted })
    } returns
      listOf(
        SourceConnection()
          .withSourceId(tombstonedSourceId)
          .withWorkspaceId(workspaceId)
          .withName("deleted postgres")
          .withTombstone(true),
      )
    every {
      destinationService.listWorkspacesDestinationConnections(match { it.workspaceIds == listOf(workspaceId) && it.includeDeleted })
    } returns
      listOf(
        DestinationConnection()
          .withDestinationId(tombstonedDestinationId)
          .withWorkspaceId(workspaceId)
          .withName("deleted bigquery")
          .withTombstone(true),
      )
    every { connectionService.getStandardSync(connectionId) } returns
      StandardSync()
        .withConnectionId(connectionId)
        .withSourceId(tombstonedSourceId)
        .withDestinationId(tombstonedDestinationId)

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)

    assertEquals(listOf(tombstonedSourceId), result.manifest.sourceIds)
    assertEquals(listOf(tombstonedDestinationId), result.manifest.destinationIds)
    assertEquals(
      listOf(DsrManifest.ManifestConnection(connectionId, tombstonedSourceId, "deleted postgres", tombstonedDestinationId, "deleted bigquery")),
      result.manifest.connectionRefs,
    )
  }

  @Test
  fun `preview includes tombstoned connector builder projects from deleted workspaces`() {
    val tombstonedBuilderProjectId = UUID.randomUUID()
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId) } answers {
      java.util.stream.Stream.of(
        ConnectorBuilderProject()
          .withBuilderProjectId(builderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("active builder project"),
      )
    }
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(workspaceId, true) } answers {
      java.util.stream.Stream.of(
        ConnectorBuilderProject()
          .withBuilderProjectId(builderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("active builder project"),
        ConnectorBuilderProject()
          .withBuilderProjectId(tombstonedBuilderProjectId)
          .withWorkspaceId(workspaceId)
          .withName("deleted builder project")
          .withTombstone(true),
      )
    }

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)

    assertEquals(listOf(builderProjectId, tombstonedBuilderProjectId), result.manifest.connectorBuilderProjectIds)
  }

  @Test
  fun `preview is idempotent when an active request already exists`() {
    val existing =
      DataSubjectDeletionRequest(
        id = UUID.randomUUID(),
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findActiveByEmailHash(emailHash) } returns Optional.of(existing)

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)
    assertEquals("ALREADY_EXISTS", result.status)
    assertEquals(existing.id, result.requestId)

    verify(exactly = 0) { deletionRequestRepository.save(any<DataSubjectDeletionRequest>()) }
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
  }

  @Test
  fun `preview throws when the user does not exist`() {
    every { userPersistence.getUserByEmail("ghost@example.com") } returns Optional.empty()
    every { deletionRequestRepository.findActiveByEmailHash(DsrDeletionService.emailHash("ghost@example.com")) } returns Optional.empty()

    assertThrows<DsrUserNotFoundException> {
      service.preview("ghost@example.com", datagrailId, oncallIssueNumber, requestedBy)
    }
  }

  @Test
  fun `startExecution claims a PREVIEWED request and does not run destructive work`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.markRunningIfPreviewed(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io", any())
    } returns 1

    val result = service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")

    assertEquals(requestId, result.requestId)
    assertEquals("RUNNING", result.status)
    assertTrue(result.started)
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `startExecution is idempotent for a matching RUNNING request`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        confirmedBy = "reviewer@airbyte.io",
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.refreshQueuedRunningIfTimedOut(
        requestId = requestId,
        email = email,
        datagrailId = datagrailId,
        oncallIssueNumber = oncallIssueNumber,
        queuedBefore = any(),
        refreshedAt = any(),
      )
    } returns 0
    every { deletionRequestTimeoutService.failTimedOutActiveRequest(requestId, Duration.ofMinutes(30)) } returns false

    val result = service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")

    assertEquals(requestId, result.requestId)
    assertEquals("RUNNING", result.status)
    assertFalse(result.started)
    verify(exactly = 0) {
      deletionRequestRepository.markRunningIfPreviewed(any(), any(), any(), any(), any(), any())
    }
    verify(exactly = 1) {
      deletionRequestTimeoutService.failTimedOutActiveRequest(requestId, Duration.ofMinutes(30))
    }
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `startExecution requeues a matching stale queued RUNNING request`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        confirmedBy = "reviewer@airbyte.io",
        confirmedAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1),
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1),
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.refreshQueuedRunningIfTimedOut(
        requestId = requestId,
        email = email,
        datagrailId = datagrailId,
        oncallIssueNumber = oncallIssueNumber,
        queuedBefore = any(),
        refreshedAt = any(),
      )
    } returns 1

    val result = service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")

    assertEquals(requestId, result.requestId)
    assertEquals("RUNNING", result.status)
    assertTrue(result.started)
    verify(exactly = 0) {
      deletionRequestRepository.markRunningIfPreviewed(any(), any(), any(), any(), any(), any())
    }
    verify(exactly = 0) {
      deletionRequestTimeoutService.failTimedOutActiveRequest(any(), any())
    }
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `startExecution marks a timed out RUNNING request failed and refuses to start another worker`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        confirmedBy = "reviewer@airbyte.io",
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.refreshQueuedRunningIfTimedOut(
        requestId = requestId,
        email = email,
        datagrailId = datagrailId,
        oncallIssueNumber = oncallIssueNumber,
        queuedBefore = any(),
        refreshedAt = any(),
      )
    } returns 0
    every { deletionRequestTimeoutService.failTimedOutActiveRequest(requestId, Duration.ofMinutes(30)) } returns true

    val error =
      assertThrows<DsrInvalidStateException> {
        service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")
      }

    assertTrue(error.message!!.contains("timed out"))
    verify(exactly = 0) {
      deletionRequestRepository.markRunningIfPreviewed(any(), any(), any(), any(), any(), any())
    }
    verify(exactly = 1) {
      deletionRequestTimeoutService.failTimedOutActiveRequest(requestId, Duration.ofMinutes(30))
    }
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `startExecution rejects terminal statuses`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.completed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)

    assertThrows<DsrInvalidStateException> {
      service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")
    }
  }

  @Test
  fun `execute rejects when confirmation inputs do not match the previewed request`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)

    assertThrows<DsrInvalidConfirmationException> {
      service.startExecution(requestId, "other@example.com", datagrailId, oncallIssueNumber, "reviewer@airbyte.io")
    }
    assertThrows<DsrInvalidConfirmationException> {
      service.startExecution(requestId, email, "wrong-dg-id", oncallIssueNumber, "reviewer@airbyte.io")
    }
    assertThrows<DsrInvalidConfirmationException> {
      service.startExecution(requestId, email, datagrailId, "ONCALL-9999", "reviewer@airbyte.io")
    }
  }

  @Test
  fun `execute refuses to run destructive work when the atomic running claim fails`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.markRunningIfPreviewed(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io", any())
    } returns 0

    assertThrows<DsrInvalidStateException> {
      service.startExecution(requestId, email, datagrailId, oncallIssueNumber, "reviewer@airbyte.io")
    }

    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `execute fails before destructive work when the preview manifest is stale`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    val finalizations = captureActiveFinalization(requestId)

    val result = service.executeClaimedRequest(requestId)

    assertEquals("FAILED", result.status)
    assertEquals(0, result.deletedConnectionsCount)
    assertTrue(result.errors.single().contains("Preview manifest is stale"))
    assertEquals(DataSubjectDeletionStatus.failed, finalizations.single().finalStatus)
    assertEquals(datagrailId, finalizations.single().scrubbedEmail)
    val executionCounts = objectMapper.readTree(finalizations.single().executionCounts)
    assertEquals(0, executionCounts.get("deleted_jobs_count").asInt())
    assertEquals(0, executionCounts.get("deleted_keycloak_user_count").asInt())
    assertFalse(finalizations.single().scrubbedManifest.contains(email))
    assertFalse(finalizations.single().scrubbedManifest.contains(connectionId.toString()))

    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { sourceHandler.deleteSource(any<SourceIdRequestBody>()) }
    verify(exactly = 0) { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `execute fails claimed request when stored preview manifest cannot be parsed`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = "{not valid json",
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    val finalizations = captureActiveFinalization(requestId)

    val result = service.executeClaimedRequest(requestId)

    assertEquals("FAILED", result.status)
    assertTrue(result.errors.single().contains("Stored preview manifest could not be parsed"))
    assertEquals(DataSubjectDeletionStatus.failed, finalizations.single().finalStatus)
    assertEquals(datagrailId, finalizations.single().scrubbedEmail)
    val executionCounts = objectMapper.readTree(finalizations.single().executionCounts)
    assertEquals(0, executionCounts.get("deleted_attempts_count").asInt())
    assertFalse(executionCounts.has("errors"))

    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { sourceHandler.deleteSource(any<SourceIdRequestBody>()) }
    verify(exactly = 0) { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `executeClaimedRequest refuses duplicate active workers before destructive work`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        confirmedAt = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(5),
        updatedAt = OffsetDateTime.now(ZoneOffset.UTC).minusMinutes(4),
        manifest = objectMapper.writeValueAsString(manifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every { deletionRequestRepository.markRunningExecutionStarted(requestId, any()) } returns 0

    val error =
      assertThrows<DsrInvalidStateException> {
        service.executeClaimedRequest(requestId)
      }

    assertTrue(error.message!!.contains("already being executed"))
    verify(exactly = 0) { connectionManagerUtils.terminateWorkflow(any<UUID>(), any()) }
    verify(exactly = 0) { dbPrune.pruneJobsAndAttemptsByScopes(any()) }
    verify(exactly = 0) { sourceHandler.deleteSource(any<SourceIdRequestBody>()) }
    verify(exactly = 0) { destinationHandler.deleteDestination(any<DestinationIdRequestBody>()) }
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
    verify(exactly = 0) {
      executionHeartbeatExecutor.scheduleAtFixedRate(any(), any(), any(), any())
    }
  }

  @Test
  fun `preview includes every workspace under an owned organization`() {
    val organizationWorkspaceId = UUID.randomUUID()
    val organizationConnectionId = UUID.randomUUID()
    val organizationSourceId = UUID.randomUUID()
    val organizationDestinationId = UUID.randomUUID()
    every { workspaceRepository.findByOrganizationIdIn(listOf(orgId)) } returns
      listOf(
        Workspace(
          id = organizationWorkspaceId,
          name = "org-workspace",
          slug = "org-workspace",
          email = "teammate@airbyte.io",
          dataplaneGroupId = UUID.randomUUID(),
          organizationId = orgId,
        ),
      )
    every { connectionService.listConnectionIdsForWorkspace(organizationWorkspaceId) } returns listOf(organizationConnectionId)
    every { connectionService.getStandardSync(organizationConnectionId) } returns
      StandardSync()
        .withConnectionId(organizationConnectionId)
        .withSourceId(organizationSourceId)
        .withDestinationId(organizationDestinationId)
    every { sourceService.listWorkspaceSourceConnection(organizationWorkspaceId) } returns
      listOf(SourceConnection().withSourceId(organizationSourceId).withWorkspaceId(organizationWorkspaceId).withName("org source"))
    every { sourceService.getSourceConnection(organizationSourceId) } returns
      SourceConnection().withSourceId(organizationSourceId).withWorkspaceId(organizationWorkspaceId).withName("org source")
    every { destinationService.listWorkspaceDestinationConnection(organizationWorkspaceId) } returns
      listOf(
        DestinationConnection().withDestinationId(organizationDestinationId).withWorkspaceId(organizationWorkspaceId).withName("org destination"),
      )
    every { destinationService.getDestinationConnection(organizationDestinationId) } returns
      DestinationConnection().withDestinationId(organizationDestinationId).withWorkspaceId(organizationWorkspaceId).withName("org destination")
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(organizationWorkspaceId) } answers {
      java.util.stream.Stream
        .empty()
    }
    every { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(organizationWorkspaceId, true) } answers {
      java.util.stream.Stream
        .empty()
    }
    every {
      dbPrune.countJobsAndAttemptsByScopes(listOf(connectionId.toString(), organizationConnectionId.toString()))
    } returns DbPrune.JobScopeCounts(jobCount = 11L, attemptCount = 13L)
    every { connectionManagerUtils.isWorkflowStateRunning(organizationConnectionId) } returns false

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)

    assertEquals(listOf(workspaceId, organizationWorkspaceId), result.manifest.workspaceIds)
    assertEquals(listOf(connectionId, organizationConnectionId), result.manifest.connectionIds)
    assertEquals(11L, result.manifest.jobCount)
    assertEquals(13L, result.manifest.attemptCount)
  }

  @Test
  fun `executeClaimedRequest terminates workflows before deletion and scrubs persisted PII`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
        prepareWarnings = objectMapper.writeValueAsString(listOf("No Keycloak user was found for $email")),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every { connectionManagerUtils.terminateWorkflow(connectionId, any()) } just Runs
    every { dbPrune.listSyncWorkloadIdsByScopes(listOf(connectionId.toString())) } returns
      listOf("${connectionId}_101_0_sync")
    every { dbPrune.pruneJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobDeletionCounts(deletedJobsCount = 7, deletedAttemptsCount = 9)
    every { externalUserService.deleteUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns 1
    configDeletionCounts =
      DsrDeletionService.ConfigDeletionCounts(
        connections = 1,
        actors = 2,
        builderProjects = 0,
        permissions = 1,
        workspaces = 1,
        organizations = 1,
        authUsers = 1,
        userTombstoned = true,
      )
    val finalizations = captureActiveFinalization(requestId)

    val result = service.executeClaimedRequest(requestId)

    assertEquals("COMPLETED", result.status)
    assertEquals(1, result.terminatedTemporalWorkflowCount)
    assertEquals(7, result.deletedJobsCount)
    assertEquals(9, result.deletedAttemptsCount)

    verifyOrder {
      deletionRequestRepository.markRunningExecutionStarted(requestId, any())
      executionHeartbeatExecutor.scheduleAtFixedRate(any(), 60000L, 60000L, TimeUnit.MILLISECONDS)
      connectionManagerUtils.terminateWorkflow(connectionId, any())
      dbPrune.listSyncWorkloadIdsByScopes(listOf(connectionId.toString()))
      dbPrune.pruneJobsAndAttemptsByScopes(listOf(connectionId.toString()))
      sourceHandler.deleteSource(any<SourceIdRequestBody>())
      destinationHandler.deleteDestination(any<DestinationIdRequestBody>())
      externalUserService.deleteUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM)
      executionHeartbeatFuture.cancel(false)
    }

    val finalization = finalizations.single()
    assertEquals(DataSubjectDeletionStatus.completed, finalization.finalStatus)
    assertEquals(datagrailId, finalization.scrubbedEmail)
    assertNull(finalization.confirmErrors)
    val executionCounts = objectMapper.readTree(finalization.executionCounts)
    assertEquals(7, executionCounts.get("deleted_jobs_count").asInt())
    assertEquals(9, executionCounts.get("deleted_attempts_count").asInt())
    assertEquals(1, executionCounts.get("deleted_connections_count").asInt())
    assertEquals(2, executionCounts.get("deleted_actors_count").asInt())
    assertEquals(1, executionCounts.get("deleted_keycloak_user_count").asInt())
    assertEquals(1, executionCounts.get("terminated_temporal_workflow_count").asInt())
    assertTrue(executionCounts.get("tombstoned_user").asBoolean())
    assertFalse(executionCounts.has("errors"))
    assertFalse(finalization.scrubbedManifest.contains(email))
    assertFalse(finalization.scrubbedManifest.contains("Davin"))
    assertFalse(finalization.scrubbedManifest.contains("postgres"))
    assertFalse(finalization.scrubbedManifest.contains("bigquery"))
  }

  @Test
  fun `executeClaimedRequest persists sanitized errors and partial execution counts`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every { connectionManagerUtils.terminateWorkflow(connectionId, any()) } just Runs
    every { dbPrune.listSyncWorkloadIdsByScopes(listOf(connectionId.toString())) } returns
      listOf("${connectionId}_101_0_sync")
    every { dbPrune.pruneJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobDeletionCounts(deletedJobsCount = 7, deletedAttemptsCount = 9)
    every { configDatabase.transaction<DsrDeletionService.ConfigDeletionCounts>(any()) } throws
      RuntimeException("Config DB purge failed for $email")
    val finalizations = captureActiveFinalization(requestId)

    val result = service.executeClaimedRequest(requestId)

    assertEquals("FAILED", result.status)
    assertEquals(7, result.deletedJobsCount)
    assertEquals(9, result.deletedAttemptsCount)
    assertTrue(result.errors.single().contains(datagrailId))
    assertFalse(result.errors.single().contains(email))
    assertEquals(DataSubjectDeletionStatus.failed, finalizations.single().finalStatus)
    assertFalse(finalizations.single().confirmErrors!!.contains(email))
    val executionCounts = objectMapper.readTree(finalizations.single().executionCounts)
    assertEquals(7, executionCounts.get("deleted_jobs_count").asInt())
    assertEquals(9, executionCounts.get("deleted_attempts_count").asInt())
    assertEquals(0, executionCounts.get("deleted_connections_count").asInt())
    assertEquals(1, executionCounts.get("terminated_temporal_workflow_count").asInt())
    verify(exactly = 0) { externalUserService.deleteUsersByEmailInRealm(any(), any()) }
  }

  @Test
  fun `executeClaimedRequest does not blindly overwrite terminal timeout recovery`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every { connectionManagerUtils.terminateWorkflow(connectionId, any()) } just Runs
    every { dbPrune.listSyncWorkloadIdsByScopes(listOf(connectionId.toString())) } returns
      listOf("${connectionId}_101_0_sync")
    every { dbPrune.pruneJobsAndAttemptsByScopes(listOf(connectionId.toString())) } returns
      DbPrune.JobDeletionCounts(deletedJobsCount = 7, deletedAttemptsCount = 9)
    every { externalUserService.deleteUsersByEmailInRealm(email, DsrDeletionService.CLOUD_USERS_REALM) } returns 1
    captureActiveFinalization(requestId, updatedRows = 0)

    val error =
      assertThrows<DsrInvalidStateException> {
        service.executeClaimedRequest(requestId)
      }

    assertTrue(error.message!!.contains("already reached a terminal state"))
    verify(exactly = 0) { deletionRequestRepository.update(any()) }
  }

  @Test
  fun `cancel transitions a PREVIEWED row to CANCELED and scrubs persisted PII`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
        prepareWarnings = objectMapper.writeValueAsString(listOf("No Keycloak user was found for $email")),
      )
    val canceledAtSlot = slot<java.time.OffsetDateTime>()
    val scrubbedEmailSlot = slot<String>()
    val scrubbedManifestSlot = slot<String>()
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.cancelIfPreviewed(
        requestId,
        "reviewer@airbyte.io",
        capture(canceledAtSlot),
        capture(scrubbedEmailSlot),
        capture(scrubbedManifestSlot),
      )
    } returns 1

    val result = service.cancel(requestId, canceledBy = "reviewer@airbyte.io")

    assertEquals(DataSubjectDeletionStatus.canceled, result.status)
    assertEquals("reviewer@airbyte.io", result.confirmedBy)
    assertEquals(canceledAtSlot.captured, result.confirmedAt)
    assertEquals(canceledAtSlot.captured, result.completedAt)
    assertEquals(datagrailId, result.email)
    assertEquals(null, result.prepareWarnings)
    assertEquals(null, result.confirmErrors)
    assertEquals(datagrailId, scrubbedEmailSlot.captured)
    assertEquals(result.manifest, scrubbedManifestSlot.captured)
    assertFalse(result.manifest.contains(email))
    assertFalse(result.manifest.contains("Davin"))
    assertFalse(result.manifest.contains("postgres"))
    assertFalse(result.manifest.contains("bigquery"))
    verify(exactly = 0) { deletionRequestRepository.update(any()) }
  }

  @Test
  fun `cancel refuses to overwrite a request that is no longer PREVIEWED`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.previewed,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        manifest = objectMapper.writeValueAsString(manifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    every {
      deletionRequestRepository.cancelIfPreviewed(requestId, "reviewer@airbyte.io", any(), any(), any())
    } returns 0

    val error =
      assertThrows<DsrInvalidStateException> {
        service.cancel(requestId, canceledBy = "reviewer@airbyte.io")
      }

    assertTrue(error.message!!.contains("could not be canceled"))
    verify(exactly = 0) { deletionRequestRepository.update(any()) }
  }

  @Test
  fun `manifest serializes round-trip cleanly through Jackson`() {
    val manifest = emptyManifest()
    val json = objectMapper.writeValueAsString(manifest)
    assertTrue(json.contains("\"target_email\""))
    assertTrue(json.contains("\"datagrail_id\""))
    val parsed: DsrManifest = objectMapper.readValue(json)
    assertEquals(manifest, parsed)
  }

  @Test
  fun `failRunningRequestUnexpectedly marks a running request failed and scrubs PII`() {
    val requestId = UUID.randomUUID()
    val row =
      DataSubjectDeletionRequest(
        id = requestId,
        email = email,
        emailHash = emailHash,
        datagrailId = datagrailId,
        status = DataSubjectDeletionStatus.running,
        userId = userId,
        requestedBy = requestedBy,
        oncallIssueNumber = oncallIssueNumber,
        confirmedBy = "reviewer@airbyte.io",
        manifest = objectMapper.writeValueAsString(emptyManifest()),
      )
    every { deletionRequestRepository.findById(requestId) } returns Optional.of(row)
    val finalizations = captureUnexpectedFailureFinalization(requestId)

    val result =
      service.failRunningRequestUnexpectedly(
        requestId,
        IllegalStateException("Unexpected background failure for $email"),
      )

    assertNotNull(result)
    assertEquals("FAILED", result!!.status)
    assertEquals(DataSubjectDeletionStatus.failed, finalizations.single().finalStatus)
    assertEquals(datagrailId, finalizations.single().scrubbedEmail)
    assertFalse(finalizations.single().scrubbedManifest.contains(email))
    assertFalse(finalizations.single().confirmErrors!!.contains(email))
    assertTrue(finalizations.single().confirmErrors!!.contains(datagrailId))
    val executionCounts = objectMapper.readTree(finalizations.single().executionCounts)
    assertEquals(0, executionCounts.get("deleted_jobs_count").asInt())
    assertEquals(0, executionCounts.get("deleted_keycloak_user_count").asInt())
  }

  @Test
  fun `preview warning fires when permissions exceed owned workspaces and orgs`() {
    every { permissionRepository.findByUserId(userId) } returns
      listOf(
        Permission(
          id = UUID.randomUUID(),
          userId = userId,
          workspaceId = UUID.randomUUID(),
          permissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin,
        ),
        Permission(
          id = UUID.randomUUID(),
          userId = userId,
          workspaceId = UUID.randomUUID(),
          permissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin,
        ),
        Permission(
          id = UUID.randomUUID(),
          userId = userId,
          workspaceId = UUID.randomUUID(),
          permissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType.workspace_admin,
        ),
      )

    val result = service.preview(email, datagrailId, oncallIssueNumber, requestedBy)
    assertTrue(
      result.warnings.any { it.contains("member of other tenants") },
      "Expected a warning about cross-tenant permissions, got: ${result.warnings}",
    )
  }

  private fun emptyManifest(): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin"),
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

  private fun manifest(): DsrManifest =
    DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId, email, "Davin"),
      workspaceIds = listOf(workspaceId),
      workspaceRefs = listOf(DsrManifest.ManifestWorkspace(workspaceId, "my-workspace")),
      organizationIds = listOf(orgId),
      organizationRefs = listOf(DsrManifest.ManifestOrganization(orgId, "my-org")),
      connectionIds = listOf(connectionId),
      connectionRefs = listOf(DsrManifest.ManifestConnection(connectionId, sourceId, "postgres", destinationId, "bigquery")),
      sourceIds = listOf(sourceId),
      destinationIds = listOf(destinationId),
      connectorBuilderProjectIds = emptyList(),
      permissionIds = listOf(permissionId),
      authUsers = listOf(DsrManifest.ManifestAuthUser("kc-auth-1", "KEYCLOAK")),
      jobCount = 7L,
      attemptCount = 9L,
      keycloakUsers = listOf(DsrManifest.ManifestKeycloakUser("kc-auth-1", email, "Davin", true)),
      temporalWorkflows = listOf(DsrManifest.ManifestTemporalWorkflow("connection_manager_$connectionId", connectionId, true)),
    )
}
