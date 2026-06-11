/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.dsr

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.temporal.ConnectionManagerUtils
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.repositories.DataSubjectDeletionRequestRepository
import io.airbyte.data.repositories.OrganizationRepository
import io.airbyte.data.repositories.PermissionRepository
import io.airbyte.data.repositories.WorkspaceRepository
import io.airbyte.data.repositories.entities.DataSubjectDeletionRequest
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.ExternalUserService
import io.airbyte.data.services.SourceService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.enums.DataSubjectDeletionStatus
import io.airbyte.domain.services.dsr.DsrManifest
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.persistence.job.DbPrune
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.Table
import org.jooq.impl.DSL
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Locale
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.Tables as ConfigTables

private val log = KotlinLogging.logger {}

/**
 * Orchestrates GDPR / DSR deletion as a preview-first runbook.
 *
 * The preview endpoint is intentionally read-only. It resolves every config, jobs, Keycloak, and
 * Temporal target Support needs to review before approving deletion. The execute endpoint is the
 * only destructive path and validates request id, email, DataGrail id, and on-call issue number
 * before doing any work.
 */
@Singleton
open class DsrDeletionService(
  private val userPersistence: UserPersistence,
  private val workspaceRepository: WorkspaceRepository,
  private val organizationRepository: OrganizationRepository,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val connectorBuilderService: ConnectorBuilderService,
  private val permissionRepository: PermissionRepository,
  private val externalUserService: ExternalUserService,
  private val connectionManagerUtils: ConnectionManagerUtils,
  private val sourceHandler: SourceHandler,
  private val destinationHandler: DestinationHandler,
  private val dbPrune: DbPrune,
  private val deletionRequestRepository: DataSubjectDeletionRequestRepository,
  private val objectMapper: ObjectMapper,
  private val metricClient: MetricClient,
  @Named("configDatabase") configDatabase: Database,
) {
  private val configDb = ExceptionWrappingDatabase(configDatabase)

  data class PreviewResult(
    val requestId: UUID?,
    val status: String,
    val manifest: DsrManifest,
    val warnings: List<String>,
  )

  data class ExecuteResult(
    val requestId: UUID,
    val status: String,
    val deletedJobsCount: Int,
    val deletedAttemptsCount: Int,
    val deletedConnectionsCount: Int,
    val deletedActorsCount: Int,
    val deletedBuilderProjectsCount: Int,
    val deletedPermissionsCount: Int,
    val deletedWorkspacesCount: Int,
    val deletedOrganizationsCount: Int,
    val deletedAuthUsersCount: Int,
    val tombstonedUser: Boolean,
    val deletedKeycloakUserCount: Int,
    val terminatedTemporalWorkflowCount: Int,
    val errors: List<String>,
  )

  open fun preview(
    email: String,
    datagrailId: String,
    oncallIssueNumber: String,
    requestedBy: String,
  ): PreviewResult {
    require(email.isNotBlank()) { "email must not be blank" }
    require(datagrailId.isNotBlank()) { "datagrailId must not be blank" }
    require(oncallIssueNumber.isNotBlank()) { "oncallIssueNumber must not be blank" }
    require(requestedBy.isNotBlank()) { "requestedBy must not be blank" }

    val emailHash = emailHash(email)
    val existing = deletionRequestRepository.findActiveByEmailHash(emailHash)
    if (existing.isPresent) {
      val row = existing.get()
      log.warn { "DSR preview requested for emailHash=$emailHash but request ${row.id} (${row.status}) already exists" }
      return PreviewResult(
        requestId = row.id,
        status = "ALREADY_EXISTS",
        manifest = objectMapper.readValue(row.manifest),
        warnings = listOf("A ${row.status} deletion request for this email already exists."),
      )
    }

    val manifest = buildManifest(email, datagrailId)
    val warnings = buildPreviewWarnings(manifest)

    val saved =
      deletionRequestRepository.save(
        DataSubjectDeletionRequest(
          email = email,
          emailHash = emailHash,
          datagrailId = datagrailId,
          status = DataSubjectDeletionStatus.previewed,
          userId = manifest.userId,
          requestedBy = requestedBy,
          oncallIssueNumber = oncallIssueNumber,
          manifest = objectMapper.writeValueAsString(manifest),
          prepareWarnings = if (warnings.isNotEmpty()) objectMapper.writeValueAsString(warnings) else null,
        ),
      )

    metricClient.count(metric = OssMetricsRegistry.DSR_DELETION_PREPARED)
    log.warn {
      "DSR preview captured for emailHash=$emailHash (request=${saved.id}, datagrailId=$datagrailId): " +
        "${manifest.workspaceIds.size} workspaces, ${manifest.connectionIds.size} connections, " +
        "${manifest.jobCount} jobs, ${manifest.attemptCount} attempts"
    }

    return PreviewResult(
      requestId = saved.id,
      status = "PREVIEWED",
      manifest = manifest,
      warnings = warnings,
    )
  }

  open fun execute(
    requestId: UUID,
    email: String,
    datagrailId: String,
    oncallIssueNumber: String,
    executedBy: String,
  ): ExecuteResult {
    require(email.isNotBlank()) { "email must not be blank" }
    require(datagrailId.isNotBlank()) { "datagrailId must not be blank" }
    require(oncallIssueNumber.isNotBlank()) { "oncallIssueNumber must not be blank" }
    require(executedBy.isNotBlank()) { "executedBy must not be blank" }

    val row =
      deletionRequestRepository
        .findById(requestId)
        .orElseThrow { DsrRequestNotFoundException("No DSR deletion request with id $requestId") }

    validateExecutionRequest(row, requestId, email, datagrailId, oncallIssueNumber)

    val executedAt = OffsetDateTime.now(ZoneOffset.UTC)
    val claimed =
      deletionRequestRepository.markRunningIfPreviewed(
        requestId = requestId,
        email = email,
        datagrailId = datagrailId,
        oncallIssueNumber = oncallIssueNumber,
        executedBy = executedBy,
        executedAt = executedAt,
      )
    if (claimed != 1) {
      val currentStatus = deletionRequestRepository.findById(requestId).map { it.status }.orElse(row.status)
      throw DsrInvalidStateException(
        "Deletion request $requestId could not be claimed for execution; current state is $currentStatus.",
      )
    }

    row.status = DataSubjectDeletionStatus.running
    row.confirmedBy = executedBy
    row.confirmedAt = executedAt

    val previewManifest =
      runCatching {
        objectMapper.readValue<DsrManifest>(row.manifest)
      }.getOrElse {
        return failClaimedRequest(
          row = row,
          requestId = requestId,
          manifest = minimalFailureManifest(email, datagrailId, row.userId),
          datagrailId = datagrailId,
          email = email,
          errors = listOf("Stored preview manifest could not be parsed: ${it.message}"),
        )
      }
    val targetEmailHash = emailHash(email)
    val currentManifestResult = runCatching { buildManifest(email, datagrailId) }
    if (currentManifestResult.isFailure) {
      return failClaimedRequest(
        row = row,
        requestId = requestId,
        manifest = previewManifest,
        datagrailId = datagrailId,
        email = email,
        errors = listOf("Manifest refresh failed: ${currentManifestResult.exceptionOrNull()?.message}"),
      )
    }

    val manifest = currentManifestResult.getOrThrow()
    val driftErrors = manifestScopeDriftErrors(previewManifest, manifest)
    if (driftErrors.isNotEmpty()) {
      return failClaimedRequest(
        row = row,
        requestId = requestId,
        manifest = previewManifest,
        datagrailId = datagrailId,
        email = email,
        errors = driftErrors,
      )
    }

    val errors = mutableListOf<String>()

    log.warn {
      "DSR execute starting for request=$requestId emailHash=$targetEmailHash datagrailId=$datagrailId " +
        "oncall=$oncallIssueNumber by=$executedBy. " +
        "Hard-deleting ${manifest.connectionIds.size} connections, ${manifest.workspaceIds.size} workspaces, " +
        "${manifest.organizationIds.size} orgs, ${manifest.jobCount} jobs and ${manifest.attemptCount} attempts."
    }

    val terminatedTemporalWorkflows = terminateTemporalWorkflows(manifest, oncallIssueNumber, requestId, errors)

    val connectionScopes = manifest.connectionIds.map { it.toString() }
    val syncWorkloadIds =
      if (errors.isEmpty()) {
        runCatching {
          dbPrune.listSyncWorkloadIdsByScopes(connectionScopes)
        }.getOrElse {
          log.error(it) { "DSR $requestId sync workload lookup failed" }
          errors.add("Sync workload lookup failed: ${it.message}")
          emptyList()
        }
      } else {
        emptyList()
      }

    val jobCounts =
      if (errors.isEmpty()) {
        runCatching {
          dbPrune.pruneJobsAndAttemptsByScopes(connectionScopes)
        }.getOrElse {
          log.error(it) { "DSR $requestId job purge failed" }
          errors.add("Job purge failed: ${it.message}")
          DbPrune.JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
        }
      } else {
        DbPrune.JobDeletionCounts(deletedJobsCount = 0, deletedAttemptsCount = 0)
      }

    if (errors.isEmpty()) {
      cleanupActorSecrets(manifest, errors)
    }

    val configCounts =
      if (errors.isEmpty()) {
        runCatching {
          configDb.transaction { ctx -> hardDeleteConfigRows(ctx, manifest, datagrailId, syncWorkloadIds) }
        }.getOrElse {
          log.error(it) { "DSR $requestId config DB purge failed" }
          errors.add("Config DB purge failed: ${it.message}")
          ConfigDeletionCounts.empty()
        }
      } else {
        ConfigDeletionCounts.empty()
      }

    val deletedKeycloakUsers =
      if (errors.isEmpty()) {
        runCatching {
          externalUserService.deleteUsersByEmailInRealm(email, CLOUD_USERS_REALM)
        }.getOrElse {
          log.error(it) { "DSR $requestId Keycloak delete failed for emailHash=${emailHash(email)} in $CLOUD_USERS_REALM" }
          errors.add("Keycloak delete failed: ${it.message}")
          0
        }
      } else {
        0
      }

    val finalStatus = if (errors.isEmpty()) DataSubjectDeletionStatus.completed else DataSubjectDeletionStatus.failed
    val persistedErrors = sanitizeErrors(errors, email, datagrailId)
    row.status = finalStatus
    row.completedAt = OffsetDateTime.now(ZoneOffset.UTC)
    scrubExecutedRequestRow(row, manifest, datagrailId, persistedErrors)
    deletionRequestRepository.update(row)

    if (finalStatus == DataSubjectDeletionStatus.completed) {
      metricClient.count(metric = OssMetricsRegistry.DSR_DELETION_COMPLETED)
      log.warn { "DSR execute COMPLETED for request=$requestId datagrailId=$datagrailId" }
    } else {
      metricClient.count(metric = OssMetricsRegistry.DSR_DELETION_FAILED)
      log.error { "DSR execute FAILED for request=$requestId datagrailId=$datagrailId errors=$persistedErrors" }
    }

    return ExecuteResult(
      requestId = requestId,
      status = finalStatus.literal.uppercase(),
      deletedJobsCount = jobCounts.deletedJobsCount,
      deletedAttemptsCount = jobCounts.deletedAttemptsCount,
      deletedConnectionsCount = configCounts.connections,
      deletedActorsCount = configCounts.actors,
      deletedBuilderProjectsCount = configCounts.builderProjects,
      deletedPermissionsCount = configCounts.permissions,
      deletedWorkspacesCount = configCounts.workspaces,
      deletedOrganizationsCount = configCounts.organizations,
      deletedAuthUsersCount = configCounts.authUsers,
      tombstonedUser = configCounts.userTombstoned,
      deletedKeycloakUserCount = deletedKeycloakUsers,
      terminatedTemporalWorkflowCount = terminatedTemporalWorkflows,
      errors = persistedErrors,
    )
  }

  open fun get(requestId: UUID): DataSubjectDeletionRequest? = deletionRequestRepository.findById(requestId).orElse(null)

  open fun listByEmail(email: String): List<DataSubjectDeletionRequest> = deletionRequestRepository.findAllByEmailHash(emailHash(email))

  open fun cancel(
    requestId: UUID,
    canceledBy: String,
  ): DataSubjectDeletionRequest {
    require(canceledBy.isNotBlank()) { "canceledBy must not be blank" }

    val row =
      deletionRequestRepository
        .findById(requestId)
        .orElseThrow { DsrRequestNotFoundException("No DSR deletion request with id $requestId") }
    if (row.status != DataSubjectDeletionStatus.previewed) {
      throw DsrInvalidStateException(
        "Deletion request $requestId is in state ${row.status}, only PREVIEWED requests can be canceled.",
      )
    }
    val canceledAt = OffsetDateTime.now(ZoneOffset.UTC)
    val manifest: DsrManifest = objectMapper.readValue(row.manifest)
    val scrubbedManifest = objectMapper.writeValueAsString(redactedManifest(manifest, row.datagrailId))
    val canceledRows =
      deletionRequestRepository.cancelIfPreviewed(
        requestId = requestId,
        canceledBy = canceledBy,
        canceledAt = canceledAt,
        scrubbedEmail = row.datagrailId,
        scrubbedManifest = scrubbedManifest,
      )
    if (canceledRows != 1) {
      val currentStatus = deletionRequestRepository.findById(requestId).map { it.status }.orElse(row.status)
      throw DsrInvalidStateException(
        "Deletion request $requestId could not be canceled; current state is $currentStatus.",
      )
    }
    row.status = DataSubjectDeletionStatus.canceled
    row.confirmedBy = canceledBy
    row.confirmedAt = canceledAt
    row.completedAt = canceledAt
    row.email = row.datagrailId
    row.manifest = scrubbedManifest
    row.prepareWarnings = null
    row.confirmErrors = null
    log.warn { "DSR request $requestId canceled by $canceledBy before execute" }
    return row
  }

  private fun validateExecutionRequest(
    row: DataSubjectDeletionRequest,
    requestId: UUID,
    email: String,
    datagrailId: String,
    oncallIssueNumber: String,
  ) {
    if (row.status != DataSubjectDeletionStatus.previewed) {
      throw DsrInvalidStateException(
        "Deletion request $requestId is in state ${row.status}, must be PREVIEWED to execute.",
      )
    }
    if (!row.email.equals(email, ignoreCase = true)) {
      throw DsrInvalidConfirmationException("Email on execute does not match previewed request.")
    }
    if (row.datagrailId != datagrailId) {
      throw DsrInvalidConfirmationException("DataGrail id on execute does not match previewed request.")
    }
    if (row.oncallIssueNumber != oncallIssueNumber) {
      throw DsrInvalidConfirmationException("On-call issue number on execute does not match previewed request.")
    }
  }

  private fun buildManifest(
    email: String,
    datagrailId: String,
  ): DsrManifest {
    val user =
      userPersistence
        .getUserByEmail(email)
        .orElseThrow { DsrUserNotFoundException("No user found for email hash ${emailHash(email)}") }
    val userId = user.userId

    val workspacesByEmail = workspaceRepository.findByEmailIgnoreCase(email)
    val organizations = organizationRepository.findByEmailIgnoreCase(email)
    val organizationIds = organizations.mapNotNull { it.id }.distinct()
    val workspacesByOrganization =
      if (organizationIds.isEmpty()) {
        emptyList()
      } else {
        workspaceRepository.findByOrganizationIdIn(organizationIds)
      }
    val workspaces = (workspacesByEmail + workspacesByOrganization).distinctBy { it.id }
    val workspaceIds = workspaces.mapNotNull { it.id }.distinct()

    val connectionIds = workspaceIds.flatMap { connectionService.listConnectionIdsForWorkspace(it) }.distinct()
    val sources = workspaceIds.flatMap { sourceService.listWorkspaceSourceConnection(it) }
    val sourceById = sources.mapNotNull { source -> source.sourceId?.let { it to source } }.toMap()
    val destinations = workspaceIds.flatMap { destinationService.listWorkspaceDestinationConnection(it) }
    val destinationById = destinations.mapNotNull { destination -> destination.destinationId?.let { it to destination } }.toMap()

    val standardSyncs =
      connectionIds.mapNotNull { connectionId ->
        runCatching { connectionService.getStandardSync(connectionId) }
          .onFailure { log.warn(it) { "Unable to read StandardSync while building DSR preview for connection $connectionId" } }
          .getOrNull()
      }
    val connectionRefs =
      standardSyncs.map { sync ->
        val sourceName = sync.sourceId?.let { sourceById[it]?.name ?: runCatching { sourceService.getSourceConnection(it).name }.getOrNull() }
        val destinationName =
          sync.destinationId?.let { destinationById[it]?.name ?: runCatching { destinationService.getDestinationConnection(it).name }.getOrNull() }
        DsrManifest.ManifestConnection(
          connectionId = sync.connectionId,
          sourceId = sync.sourceId,
          sourceName = sourceName,
          destinationId = sync.destinationId,
          destinationName = destinationName,
        )
      }

    val sourceIds = sources.mapNotNull { it.sourceId }.distinct()
    val destinationIds = destinations.mapNotNull { it.destinationId }.distinct()
    val builderProjectIds =
      workspaceIds
        .flatMap { connectorBuilderService.getConnectorBuilderProjectsByWorkspace(it).toList() }
        .mapNotNull { it.builderProjectId }
        .distinct()
    val permissions = permissionRepository.findByUserId(userId)
    val authUsers =
      userPersistence
        .listAuthUsersForUser(userId)
        .map { au ->
          DsrManifest.ManifestAuthUser(authUserId = au.authUserId, authProvider = au.authProvider?.toString())
        }
    val jobCounts = dbPrune.countJobsAndAttemptsByScopes(connectionIds.map { it.toString() })
    val keycloakUsers =
      externalUserService
        .findUsersByEmailInRealm(email, CLOUD_USERS_REALM)
        .map { user ->
          DsrManifest.ManifestKeycloakUser(
            authUserId = user.authUserId,
            email = user.email,
            username = user.username,
            enabled = user.enabled,
          )
        }
    val temporalWorkflows =
      connectionIds
        .filter { connectionManagerUtils.isWorkflowStateRunning(it) }
        .map { connectionId ->
          DsrManifest.ManifestTemporalWorkflow(
            workflowId = connectionManagerUtils.getConnectionManagerName(connectionId),
            connectionId = connectionId,
            running = true,
          )
        }

    return DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = userId,
      user = DsrManifest.ManifestUser(userId = userId, email = user.email, name = user.name),
      workspaceIds = workspaceIds,
      workspaceRefs = workspaces.mapNotNull { workspace -> workspace.id?.let { DsrManifest.ManifestWorkspace(it, workspace.name) } },
      organizationIds = organizationIds,
      organizationRefs = organizations.mapNotNull { org -> org.id?.let { DsrManifest.ManifestOrganization(it, org.name) } },
      connectionIds = connectionIds,
      connectionRefs = connectionRefs,
      sourceIds = sourceIds,
      destinationIds = destinationIds,
      connectorBuilderProjectIds = builderProjectIds,
      permissionIds = permissions.mapNotNull { it.id }.distinct(),
      authUsers = authUsers,
      jobCount = jobCounts.jobCount,
      attemptCount = jobCounts.attemptCount,
      keycloakUsers = keycloakUsers,
      temporalWorkflows = temporalWorkflows,
    )
  }

  private fun buildPreviewWarnings(manifest: DsrManifest): List<String> {
    val warnings = mutableListOf<String>()
    if (manifest.workspaceIds.isEmpty() && manifest.organizationIds.isEmpty() && manifest.permissionIds.isEmpty()) {
      warnings.add("User has no owned workspaces, organizations or permissions; only the user/auth rows are in scope.")
    }
    if (manifest.permissionCount > manifest.workspaceIds.size + manifest.organizationIds.size) {
      warnings.add(
        "User holds ${manifest.permissionCount} permissions, more than the ${manifest.workspaceIds.size} workspace(s) and " +
          "${manifest.organizationIds.size} organisation(s) they own. They are a member of other tenants whose permissions will also be removed.",
      )
    }
    if (manifest.jobCount > LARGE_JOB_COUNT_WARNING_THRESHOLD) {
      warnings.add("${manifest.jobCount} jobs and ${manifest.attemptCount} attempts will be hard-deleted; execute may take several minutes.")
    }
    if (manifest.keycloakUsers.isEmpty()) {
      warnings.add("No Keycloak user was found in realm $CLOUD_USERS_REALM for the target email.")
    }
    return warnings
  }

  private fun terminateTemporalWorkflows(
    manifest: DsrManifest,
    oncallIssueNumber: String,
    requestId: UUID,
    errors: MutableList<String>,
  ): Int {
    var terminated = 0
    val reason = "DSR deletion request $requestId approved in $oncallIssueNumber"
    manifest.temporalWorkflows
      .filter { it.running }
      .forEach { workflow ->
        runCatching {
          connectionManagerUtils.terminateWorkflow(workflow.connectionId, reason)
          terminated++
        }.onFailure {
          log.error(it) { "Failed to terminate Temporal workflow ${workflow.workflowId} for DSR $requestId" }
          errors.add("Temporal workflow ${workflow.workflowId} termination failed: ${it.message}")
        }
      }
    return terminated
  }

  private fun cleanupActorSecrets(
    manifest: DsrManifest,
    errors: MutableList<String>,
  ) {
    manifest.sourceIds.forEach { sourceId ->
      runCatching {
        sourceHandler.deleteSource(SourceIdRequestBody().sourceId(sourceId))
      }.onFailure {
        log.error(it) { "DSR source secret cleanup failed for source $sourceId" }
        errors.add("Source secret cleanup failed for source $sourceId: ${it.message}")
      }
    }

    manifest.destinationIds.forEach { destinationId ->
      runCatching {
        destinationHandler.deleteDestination(DestinationIdRequestBody().destinationId(destinationId))
      }.onFailure {
        log.error(it) { "DSR destination secret cleanup failed for destination $destinationId" }
        errors.add("Destination secret cleanup failed for destination $destinationId: ${it.message}")
      }
    }
  }

  private fun manifestScopeDriftErrors(
    previewManifest: DsrManifest,
    currentManifest: DsrManifest,
  ): List<String> {
    val changedScopes =
      listOfNotNull(
        if (previewManifest.userId != currentManifest.userId) "userId" else null,
        changedUuidScope("workspaceIds", previewManifest.workspaceIds, currentManifest.workspaceIds),
        changedUuidScope("organizationIds", previewManifest.organizationIds, currentManifest.organizationIds),
        changedUuidScope("connectionIds", previewManifest.connectionIds, currentManifest.connectionIds),
        changedUuidScope("sourceIds", previewManifest.sourceIds, currentManifest.sourceIds),
        changedUuidScope("destinationIds", previewManifest.destinationIds, currentManifest.destinationIds),
        changedUuidScope(
          "connectorBuilderProjectIds",
          previewManifest.connectorBuilderProjectIds,
          currentManifest.connectorBuilderProjectIds,
        ),
        changedUuidScope("permissionIds", previewManifest.permissionIds, currentManifest.permissionIds),
        changedStringScope(
          "authUsers",
          previewManifest.authUsers.map { it.authUserId },
          currentManifest.authUsers.map { it.authUserId },
        ),
        changedStringScope(
          "keycloakUsers",
          previewManifest.keycloakUsers.map { it.authUserId },
          currentManifest.keycloakUsers.map { it.authUserId },
        ),
      )

    return if (changedScopes.isEmpty()) {
      emptyList()
    } else {
      listOf("Preview manifest is stale: ${changedScopes.joinToString(", ")} changed. Run preview again before execute.")
    }
  }

  private fun changedUuidScope(
    name: String,
    previewIds: List<UUID>,
    currentIds: List<UUID>,
  ): String? = if (previewIds.map { it.toString() }.sorted() == currentIds.map { it.toString() }.sorted()) null else name

  private fun changedStringScope(
    name: String,
    previewIds: List<String>,
    currentIds: List<String>,
  ): String? = if (previewIds.sorted() == currentIds.sorted()) null else name

  private fun failClaimedRequest(
    row: DataSubjectDeletionRequest,
    requestId: UUID,
    manifest: DsrManifest,
    datagrailId: String,
    email: String,
    errors: List<String>,
  ): ExecuteResult {
    val persistedErrors = sanitizeErrors(errors, email, datagrailId)
    row.status = DataSubjectDeletionStatus.failed
    row.completedAt = OffsetDateTime.now(ZoneOffset.UTC)
    scrubExecutedRequestRow(row, manifest, datagrailId, persistedErrors)
    deletionRequestRepository.update(row)

    metricClient.count(metric = OssMetricsRegistry.DSR_DELETION_FAILED)
    log.error { "DSR execute FAILED for request=$requestId datagrailId=$datagrailId errors=$persistedErrors" }

    return ExecuteResult(
      requestId = requestId,
      status = DataSubjectDeletionStatus.failed.literal.uppercase(),
      deletedJobsCount = 0,
      deletedAttemptsCount = 0,
      deletedConnectionsCount = 0,
      deletedActorsCount = 0,
      deletedBuilderProjectsCount = 0,
      deletedPermissionsCount = 0,
      deletedWorkspacesCount = 0,
      deletedOrganizationsCount = 0,
      deletedAuthUsersCount = 0,
      tombstonedUser = false,
      deletedKeycloakUserCount = 0,
      terminatedTemporalWorkflowCount = 0,
      errors = persistedErrors,
    )
  }

  private fun minimalFailureManifest(
    email: String,
    datagrailId: String,
    userId: UUID?,
  ): DsrManifest {
    val resolvedUserId = userId ?: UUID(0L, 0L)
    return DsrManifest(
      targetEmail = email,
      datagrailId = datagrailId,
      userId = resolvedUserId,
      user = DsrManifest.ManifestUser(resolvedUserId, email, null),
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

  private fun sanitizeErrors(
    errors: List<String>,
    email: String,
    datagrailId: String,
  ): List<String> {
    if (errors.isEmpty()) {
      return emptyList()
    }
    val emailRegex = Regex(Regex.escape(email), RegexOption.IGNORE_CASE)
    return errors.map { it.replace(emailRegex, datagrailId) }
  }

  private fun scrubExecutedRequestRow(
    row: DataSubjectDeletionRequest,
    manifest: DsrManifest,
    datagrailId: String,
    persistedErrors: List<String>,
  ) {
    row.email = datagrailId
    row.manifest = objectMapper.writeValueAsString(redactedManifest(manifest, datagrailId))
    row.prepareWarnings = null
    row.confirmErrors = if (persistedErrors.isEmpty()) null else objectMapper.writeValueAsString(persistedErrors)
  }

  private fun redactedManifest(
    manifest: DsrManifest,
    datagrailId: String,
  ): DsrManifest =
    manifest.copy(
      targetEmail = datagrailId,
      user = manifest.user?.copy(email = datagrailId, name = datagrailId),
      workspaceRefs = manifest.workspaceRefs.map { it.copy(name = null) },
      organizationRefs = manifest.organizationRefs.map { it.copy(name = null) },
      connectionRefs =
        manifest.connectionRefs.map {
          it.copy(sourceName = null, destinationName = null)
        },
      keycloakUsers =
        manifest.keycloakUsers.map {
          it.copy(email = null, username = null)
        },
    )

  protected open fun hardDeleteConfigRows(
    ctx: DSLContext,
    manifest: DsrManifest,
    datagrailId: String,
    syncWorkloadIds: List<String>,
  ): ConfigDeletionCounts {
    ctx.execute("SET LOCAL statement_timeout = '600s'")

    val workspaceIds = manifest.workspaceIds.distinct()
    val organizationIds = manifest.organizationIds.distinct()
    val connectionIds = manifest.connectionIds.distinct()
    val actorIdsFromManifest = (manifest.sourceIds + manifest.destinationIds).distinct()
    val actorIdsFromWorkspaces =
      if (workspaceIds.isEmpty()) {
        emptyList()
      } else {
        ctx
          .select(ConfigTables.ACTOR.ID)
          .from(ConfigTables.ACTOR)
          .where(ConfigTables.ACTOR.WORKSPACE_ID.`in`(workspaceIds))
          .fetch(ConfigTables.ACTOR.ID)
      }
    val actorIds = (actorIdsFromManifest + actorIdsFromWorkspaces).distinct()
    val groupIds = selectUuidByUuidField(ctx, "group", "id", "organization_id", organizationIds)
    val dataplaneGroupIds = selectUuidByUuidField(ctx, "dataplane_group", "id", "organization_id", organizationIds)
    val dataplaneIds = selectUuidByUuidField(ctx, "dataplane", "id", "dataplane_group_id", dataplaneGroupIds)
    val orchestrationIds = selectUuidByUuidField(ctx, "orchestration", "id", "workspace_id", workspaceIds)
    val orchestrationRunIds = selectUuidByUuidField(ctx, "orchestration_run", "id", "orchestration_id", orchestrationIds)
    val actorDefinitionIds = selectUuidByUuidField(ctx, "actor", "actor_definition_id", "id", actorIds)
    val workspaceOrganizationIds = selectUuidByUuidField(ctx, "workspace", "organization_id", "id", workspaceIds)
    val workloadIds =
      (
        syncWorkloadIds +
          selectDsrWorkloadIds(ctx, connectionIds, actorIds, actorDefinitionIds, workspaceIds, organizationIds)
      ).distinct()
    val observabilityJobIds =
      selectLongByUuidField(
        ctx = ctx,
        tableName = "observability_jobs_stats",
        selectFieldName = "job_id",
        whereFieldName = "connection_id",
        ids = connectionIds,
      )

    deleteByLongField(ctx, "observability_stream_stats", "job_id", observabilityJobIds)
    deleteByLongField(ctx, "observability_jobs_stats", "job_id", observabilityJobIds)
    deleteDataWorkerUsageRows(ctx, "data_worker_usage_reservation", workspaceIds, organizationIds, workspaceOrganizationIds)
    deleteDataWorkerUsageRows(ctx, "data_worker_usage", workspaceIds, organizationIds, workspaceOrganizationIds)

    deleteByUuidField(ctx, "connection_operation", "connection_id", connectionIds)
    deleteByUuidField(ctx, "connection_tag", "connection_id", connectionIds)
    deleteByUuidField(ctx, "connection_timeline_event", "connection_id", connectionIds)
    deleteByUuidField(ctx, "notification_configuration", "connection_id", connectionIds)
    deleteByUuidField(ctx, "schema_management", "connection_id", connectionIds)
    deleteByUuidField(ctx, "state", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_generation", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_refreshes", "connection_id", connectionIds)
    deleteByUuidField(ctx, "stream_reset", "connection_id", connectionIds)

    val connectionsDeleted =
      if (connectionIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.CONNECTION)
          .where(ConfigTables.CONNECTION.ID.`in`(connectionIds))
          .execute()
      }

    deleteByUuidField(ctx, "actor_catalog_fetch_event", "actor_id", actorIds)

    val builderProjectConditions =
      listOfNotNull(
        manifest.connectorBuilderProjectIds.distinct().takeIf { it.isNotEmpty() }?.let {
          ConfigTables.CONNECTOR_BUILDER_PROJECT.ID.`in`(it)
        },
        workspaceIds.takeIf { it.isNotEmpty() }?.let {
          ConfigTables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.`in`(it)
        },
      )
    val builderProjectsDeleted =
      if (builderProjectConditions.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.CONNECTOR_BUILDER_PROJECT)
          .where(orCondition(builderProjectConditions))
          .execute()
      }

    deleteByUuidField(ctx, "actor_oauth_parameter", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "actor_oauth_parameter", "organization_id", organizationIds)

    val actorsDeleted =
      if (actorIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.ACTOR)
          .where(ConfigTables.ACTOR.ID.`in`(actorIds))
          .execute()
      }

    deleteByUuidField(ctx, "orchestration_task_run", "orchestration_run_id", orchestrationRunIds)
    deleteByUuidField(ctx, "orchestration_run", "orchestration_id", orchestrationIds)
    deleteByUuidField(ctx, "orchestration_task", "orchestration_id", orchestrationIds)
    deleteByUuidField(ctx, "orchestration", "id", orchestrationIds)

    deleteByStringField(ctx, "workload_queue", "workload_id", workloadIds)
    deleteByStringField(ctx, "workload", "id", workloadIds)

    deleteByUuidField(ctx, "actor_definition_workspace_grant", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "actor_definition_workspace_grant", "scope_id", workspaceIds + organizationIds)
    deleteByUuidField(ctx, "secret_persistence_config", "scope_id", workspaceIds + organizationIds)

    deleteByUuidField(ctx, "user_invitation", "accepted_by_user_id", listOf(manifest.userId))
    deleteByUuidField(ctx, "user_invitation", "scope_id", workspaceIds + organizationIds)

    deleteByUuidField(ctx, "group_member", "user_id", listOf(manifest.userId))
    deleteByUuidField(ctx, "group_member", "group_id", groupIds)

    val permissionConditions = mutableListOf<Condition>(ConfigTables.PERMISSION.USER_ID.eq(manifest.userId))
    if (workspaceIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.WORKSPACE_ID.`in`(workspaceIds))
    }
    if (organizationIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.ORGANIZATION_ID.`in`(organizationIds))
    }
    if (groupIds.isNotEmpty()) {
      permissionConditions.add(ConfigTables.PERMISSION.GROUP_ID.`in`(groupIds))
    }
    val permissionsDeleted =
      ctx
        .deleteFrom(ConfigTables.PERMISSION)
        .where(orCondition(permissionConditions))
        .execute()

    deleteByUuidField(ctx, "group", "id", groupIds)

    deleteByUuidField(ctx, "workspace_service_account", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "tag", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "operation", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "private_link", "workspace_id", workspaceIds)
    deleteByUuidField(ctx, "private_link", "dataplane_group_id", dataplaneGroupIds)
    deleteWhereAnyUuidMatches(
      ctx,
      "scoped_configuration",
      listOf(
        "resource_id" to connectionIds + actorIds + workspaceIds + organizationIds,
        "scope_id" to workspaceIds + organizationIds,
      ),
    )

    deleteByUuidField(ctx, "dataplane_client_credentials", "dataplane_id", dataplaneIds)
    deleteByUuidField(ctx, "dataplane_heartbeat_log", "dataplane_id", dataplaneIds)
    deleteByUuidField(ctx, "dataplane", "dataplane_group_id", dataplaneGroupIds)
    deleteByUuidField(ctx, "dataplane_network_config", "dataplane_group_id", dataplaneGroupIds)
    deleteByUuidField(ctx, "dataplane_group", "organization_id", organizationIds)

    deleteByUuidField(ctx, "organization_domain_verification", "organization_id", organizationIds)
    deleteByUuidField(ctx, "organization_domain_verification", "created_by", listOf(manifest.userId))
    deleteByUuidField(ctx, "organization_email_domain", "organization_id", organizationIds)
    deleteByUuidField(ctx, "organization_payment_config", "organization_id", organizationIds)
    deleteByUuidField(ctx, "sso_config", "organization_id", organizationIds)

    deleteByStringField(ctx, "application", "auth_user_id", manifest.authUsers.map { it.authUserId })

    val authUsersDeleted =
      ctx
        .deleteFrom(ConfigTables.AUTH_USER)
        .where(ConfigTables.AUTH_USER.USER_ID.eq(manifest.userId))
        .execute()

    val userUpdated =
      ctx
        .update(ConfigTables.USER)
        .set(ConfigTables.USER.NAME, datagrailId)
        .set(ConfigTables.USER.EMAIL, datagrailId)
        .set(ConfigTables.USER.DEFAULT_WORKSPACE_ID, null as UUID?)
        .where(ConfigTables.USER.ID.eq(manifest.userId))
        .execute()
    if (userUpdated != 1) {
      throw IllegalStateException(
        "Expected to tombstone exactly one Airbyte user row for DSR userId=${manifest.userId}, but updated $userUpdated.",
      )
    }

    val workspacesDeleted =
      if (workspaceIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.WORKSPACE)
          .where(ConfigTables.WORKSPACE.ID.`in`(workspaceIds))
          .execute()
      }

    val organizationsDeleted =
      if (organizationIds.isEmpty()) {
        0
      } else {
        ctx
          .deleteFrom(ConfigTables.ORGANIZATION)
          .where(ConfigTables.ORGANIZATION.ID.`in`(organizationIds))
          .execute()
      }

    return ConfigDeletionCounts(
      connections = connectionsDeleted,
      actors = actorsDeleted,
      builderProjects = builderProjectsDeleted,
      permissions = permissionsDeleted,
      workspaces = workspacesDeleted,
      organizations = organizationsDeleted,
      authUsers = authUsersDeleted,
      userTombstoned = true,
    )
  }

  private fun namedTable(tableName: String): Table<*> = DSL.table(DSL.name("public", tableName))

  private fun uuidField(
    tableName: String,
    fieldName: String,
  ): Field<UUID> = DSL.field(DSL.name("public", tableName, fieldName), UUID::class.java)

  private fun stringField(
    tableName: String,
    fieldName: String,
  ): Field<String> = DSL.field(DSL.name("public", tableName, fieldName), String::class.java)

  private fun longField(
    tableName: String,
    fieldName: String,
  ): Field<Long> = DSL.field(DSL.name("public", tableName, fieldName), Long::class.java)

  private fun selectUuidByUuidField(
    ctx: DSLContext,
    tableName: String,
    selectFieldName: String,
    whereFieldName: String,
    ids: List<UUID>,
  ): List<UUID> {
    if (ids.isEmpty()) {
      return emptyList()
    }
    val selectField = uuidField(tableName, selectFieldName)
    return ctx
      .select(selectField)
      .from(namedTable(tableName))
      .where(uuidField(tableName, whereFieldName).`in`(ids))
      .fetch(selectField)
      .filterNotNull()
      .distinct()
  }

  private fun selectLongByUuidField(
    ctx: DSLContext,
    tableName: String,
    selectFieldName: String,
    whereFieldName: String,
    ids: List<UUID>,
  ): List<Long> {
    if (ids.isEmpty()) {
      return emptyList()
    }
    val selectField = longField(tableName, selectFieldName)
    return ctx
      .select(selectField)
      .from(namedTable(tableName))
      .where(uuidField(tableName, whereFieldName).`in`(ids.distinct()))
      .fetch(selectField)
      .filterNotNull()
      .distinct()
  }

  /**
   * Finds workload rows tied to the DSR target that are not guaranteed to be discoverable from
   * jobs/attempts. Workload IDs encode their owner as a string prefix, for example:
   * `<connectionId>_..._sync`, `<actorId>_..._check`, or `<actorDefinitionId>_..._discover`.
   *
   * The workspace/organization scope is required so a prefix match cannot delete workload rows
   * outside the DSR target's tenant boundary.
   */
  private fun selectDsrWorkloadIds(
    ctx: DSLContext,
    connectionIds: List<UUID>,
    actorIds: List<UUID>,
    actorDefinitionIds: List<UUID>,
    workspaceIds: List<UUID>,
    organizationIds: List<UUID>,
  ): List<String> {
    val prefixes = (connectionIds + actorIds + actorDefinitionIds).map { "${it}_" }.distinct()
    if (prefixes.isEmpty()) {
      return emptyList()
    }

    val scopeCondition = workloadScopeCondition(workspaceIds, organizationIds)
    if (scopeCondition == null) {
      log.warn {
        "Skipping DSR workload prefix lookup because workspace and organization scope are empty. " +
          "connectionCount=${connectionIds.size}, actorCount=${actorIds.size}, actorDefinitionCount=${actorDefinitionIds.size}"
      }
      return emptyList()
    }

    val workloadIds =
      selectStringByPrefixRanges(
        ctx = ctx,
        tableName = "workload",
        selectFieldName = "id",
        prefixes = prefixes,
        extraCondition = scopeCondition,
      )
    validateDsrWorkloadIdsMatchExpectedPrefixes(workloadIds, prefixes)
    return workloadIds
  }

  /**
   * Fails closed if the range query returns an ID that does not literally start with one of the
   * prefixes this delete path intended to scan. That protects the destructive delete below from
   * unexpected collation or range-boundary behavior.
   */
  private fun validateDsrWorkloadIdsMatchExpectedPrefixes(
    workloadIds: List<String>,
    prefixes: List<String>,
  ) {
    val distinctPrefixes = prefixes.distinct()
    val unexpectedWorkloadIds =
      workloadIds
        .filterNot { workloadId -> distinctPrefixes.any { prefix -> workloadId.startsWith(prefix) } }
        .distinct()
    check(unexpectedWorkloadIds.isEmpty()) {
      "DSR workload prefix lookup returned ${unexpectedWorkloadIds.size} unexpected workload ID(s) outside expected prefixes: " +
        unexpectedWorkloadIds.take(10).joinToString(", ")
    }
  }

  /**
   * Limits the workload prefix scan to the workspaces or organizations being deleted. Returning
   * null tells the caller to skip the scan rather than run an unscoped workload lookup.
   */
  private fun workloadScopeCondition(
    workspaceIds: List<UUID>,
    organizationIds: List<UUID>,
  ): Condition? {
    val scopeConditions =
      listOfNotNull(
        workspaceIds.distinct().takeIf { it.isNotEmpty() }?.let { uuidField("workload", "workspace_id").`in`(it) },
        organizationIds.distinct().takeIf { it.isNotEmpty() }?.let { uuidField("workload", "organization_id").`in`(it) },
      )
    return scopeConditions.takeIf { it.isNotEmpty() }?.let(::orCondition)
  }

  /**
   * Selects string IDs that start with any of the provided prefixes. Prefixes are batched to keep
   * the generated OR condition bounded, and each batch is additionally constrained by
   * [extraCondition] when a caller provides one.
   */
  private fun selectStringByPrefixRanges(
    ctx: DSLContext,
    tableName: String,
    selectFieldName: String,
    prefixes: List<String>,
    extraCondition: Condition? = null,
  ): List<String> {
    val distinctPrefixes = prefixes.distinct()
    if (distinctPrefixes.isEmpty()) {
      return emptyList()
    }
    val selectField = stringField(tableName, selectFieldName)
    return distinctPrefixes
      .chunked(MAX_PREFIX_RANGE_CONDITIONS)
      .flatMap { prefixBatch ->
        val conditions =
          prefixBatch.map { prefix ->
            prefixRangeCondition(selectField, prefix)
          }
        ctx
          .select(selectField)
          .from(namedTable(tableName))
          .where(extraCondition?.let { orCondition(conditions).and(it) } ?: orCondition(conditions))
          .fetch(selectField)
          .filterNotNull()
      }.distinct()
  }

  /**
   * Matches a prefix with a half-open lexicographic range instead of `LIKE`. The explicit C
   * collation makes the ASCII range math deterministic for workload IDs.
   */
  private fun prefixRangeCondition(
    field: Field<String>,
    prefix: String,
  ): Condition =
    DSL.condition(
      "{0} COLLATE \"C\" >= {1} COLLATE \"C\" AND {0} COLLATE \"C\" < {2} COLLATE \"C\"",
      field,
      DSL.inline(prefix),
      DSL.inline(prefixUpperBound(prefix)),
    )

  private fun deleteDataWorkerUsageRows(
    ctx: DSLContext,
    tableName: String,
    workspaceIds: List<UUID>,
    organizationIds: List<UUID>,
    workspaceOrganizationIds: List<UUID>,
  ): Int {
    val deletedByOrganization = deleteByUuidField(ctx, tableName, "organization_id", organizationIds)
    val deletedOrganizationIds = organizationIds.toSet()
    val workspaceScopedOrganizationIds = workspaceOrganizationIds.filterNot { deletedOrganizationIds.contains(it) }.distinct()
    if (workspaceIds.isEmpty() || workspaceScopedOrganizationIds.isEmpty()) {
      return deletedByOrganization
    }

    val deletedByWorkspace =
      ctx
        .deleteFrom(namedTable(tableName))
        .where(uuidField(tableName, "organization_id").`in`(workspaceScopedOrganizationIds))
        .and(uuidField(tableName, "workspace_id").`in`(workspaceIds.distinct()))
        .execute()
    return deletedByOrganization + deletedByWorkspace
  }

  private fun deleteByUuidField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<UUID>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(uuidField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteByStringField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<String>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(stringField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteByLongField(
    ctx: DSLContext,
    tableName: String,
    fieldName: String,
    ids: List<Long>,
  ): Int {
    if (ids.isEmpty()) {
      return 0
    }
    return ctx
      .deleteFrom(namedTable(tableName))
      .where(longField(tableName, fieldName).`in`(ids))
      .execute()
  }

  private fun deleteWhereAnyUuidMatches(
    ctx: DSLContext,
    tableName: String,
    filters: List<Pair<String, List<UUID>>>,
  ): Int {
    val condition = anyUuidMatchCondition(tableName, filters) ?: return 0
    return ctx.deleteFrom(namedTable(tableName)).where(condition).execute()
  }

  private fun anyUuidMatchCondition(
    tableName: String,
    filters: List<Pair<String, List<UUID>>>,
  ): Condition? {
    val conditions =
      filters
        .filter { (_, ids) -> ids.isNotEmpty() }
        .map { (fieldName, ids) -> uuidField(tableName, fieldName).`in`(ids.distinct()) }
    return if (conditions.isEmpty()) null else orCondition(conditions)
  }

  private fun orCondition(conditions: List<Condition>): Condition = conditions.reduce { left, right -> left.or(right) }

  data class ConfigDeletionCounts(
    val connections: Int,
    val actors: Int,
    val builderProjects: Int,
    val permissions: Int,
    val workspaces: Int,
    val organizations: Int,
    val authUsers: Int,
    val userTombstoned: Boolean,
  ) {
    companion object {
      fun empty() =
        ConfigDeletionCounts(
          connections = 0,
          actors = 0,
          builderProjects = 0,
          permissions = 0,
          workspaces = 0,
          organizations = 0,
          authUsers = 0,
          userTombstoned = false,
        )
    }
  }

  companion object {
    const val CLOUD_USERS_REALM = "_airbyte-cloud-users"
    private const val LARGE_JOB_COUNT_WARNING_THRESHOLD = 10_000L
    private const val MAX_PREFIX_RANGE_CONDITIONS = 100

    fun emailHash(email: String): String {
      val normalizedEmail = email.trim().lowercase(Locale.US)
      val digest =
        MessageDigest
          .getInstance("SHA-256")
          .digest(normalizedEmail.toByteArray(StandardCharsets.UTF_8))
      return digest.joinToString("") { "%02x".format(it.toInt() and 0xff) }
    }

    // Workload prefixes end in "_" and "`" is the next ASCII character, so [uuid_, uuid`)
    // covers exactly the workload IDs that start with uuid_ under C collation.
    private fun prefixUpperBound(prefix: String): String = prefix.dropLast(1) + "`"
  }
}

class DsrUserNotFoundException(
  message: String,
) : RuntimeException(message)

class DsrRequestNotFoundException(
  message: String,
) : RuntimeException(message)

class DsrInvalidStateException(
  message: String,
) : RuntimeException(message)

class DsrInvalidConfirmationException(
  message: String,
) : RuntimeException(message)
