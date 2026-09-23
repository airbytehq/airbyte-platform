/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.logging

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.storage.DocumentType
import io.airbyte.config.WorkloadType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.auth.TokenType
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DataplaneService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FlexSyncLogging
import io.airbyte.featureflag.Organization
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.airbyte.micronaut.runtime.StorageType
import io.airbyte.workload.api.domain.LogUploadAuthorization
import io.airbyte.workload.api.domain.Workload
import io.airbyte.workload.api.domain.WorkloadStatus
import io.airbyte.workload.errors.ConflictException
import io.airbyte.workload.errors.ForbiddenException
import io.airbyte.workload.errors.InvalidStatusTransitionException
import io.airbyte.workload.errors.LogUploadAuthorizationBrokerException
import io.airbyte.workload.errors.NotFoundException
import io.airbyte.workload.errors.UnauthorizedException
import io.airbyte.workload.handler.WorkloadHandler
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class LogUploadAuthorizationService(
  private val workloadHandler: WorkloadHandler,
  private val roleResolver: RoleResolver,
  private val dataplaneService: DataplaneService,
  private val dataplaneGroupService: DataplaneGroupService,
  private val featureFlagClient: FeatureFlagClient,
  private val storageConfig: AirbyteStorageConfig,
  private val credentialBroker: LogUploadCredentialBroker,
  private val metricClient: MetricClient,
) {
  open fun authorize(workloadId: String): LogUploadAuthorization? {
    val startedAt = System.nanoTime()
    var outcome = OUTCOME_REJECTED

    try {
      val subject = currentServiceAccountSubject()
      val workload = loadWorkload(workloadId)
      val organizationId = workload.organizationId ?: rejectNotFound()
      val status = workload.status ?: rejectNotFound()
      validateIdentityAndTopology(
        workload = workload,
        organizationId = organizationId,
        subject = subject,
        allowUnassignedDataplane = status == WorkloadStatus.PENDING,
      )
      validateLifecycle(workload)

      if (!featureFlagClient.boolVariation(FlexSyncLogging, Organization(organizationId))) {
        outcome = OUTCOME_DISABLED
        return null
      }

      if (storageConfig.type != StorageType.GCS || storageConfig.bucket.log.isBlank()) {
        rejectNotFound()
      }
      val objectKeyPrefix = objectKeyPrefix(workload.logPath)

      return try {
        credentialBroker.issue(storageConfig.bucket.log, objectKeyPrefix).also {
          outcome = OUTCOME_ISSUED
        }
      } catch (_: Exception) {
        outcome = OUTCOME_BROKER_ERROR
        throw LogUploadAuthorizationBrokerException()
      }
    } finally {
      val attributes =
        arrayOf(
          MetricAttribute(MetricTags.ROUTE, LOG_UPLOAD_AUTHORIZATION_ROUTE),
          MetricAttribute(MetricTags.OUTCOME, outcome),
        )
      metricClient.count(
        metric = OssMetricsRegistry.WORKLOAD_LOG_UPLOAD_AUTHORIZATION,
        attributes = attributes,
      )
      metricClient.distribution(
        metric = OssMetricsRegistry.WORKLOAD_LOG_UPLOAD_AUTHORIZATION_LATENCY_MS,
        value = (System.nanoTime() - startedAt) / NANOS_PER_MILLISECOND,
        attributes = attributes,
      )
    }
  }

  private fun loadWorkload(workloadId: String): Workload =
    try {
      workloadHandler.getWorkload(workloadId)
    } catch (_: NotFoundException) {
      rejectNotFound()
    }

  private fun currentServiceAccountSubject(): RoleResolver.Subject {
    val subject =
      roleResolver.newRequest().withCurrentAuthentication().subject
        ?: throw UnauthorizedException(UNAVAILABLE_MESSAGE)
    if (subject.type != TokenType.SERVICE_ACCOUNT) {
      rejectNotFound()
    }
    return subject
  }

  private fun validateIdentityAndTopology(
    workload: Workload,
    organizationId: UUID,
    subject: RoleResolver.Subject,
    allowUnassignedDataplane: Boolean,
  ) {
    val serviceAccountId = subject.id.toUuidOrNotFound()
    val workloadDataplaneId =
      workload.dataplaneId?.toUuidOrNotFound()
        ?: if (allowUnassignedDataplane) null else rejectNotFound()
    val workloadDataplaneGroupId = workload.dataplaneGroup?.toUuidOrNotFound() ?: rejectNotFound()
    val dataplane =
      try {
        dataplaneService.getDataplaneByServiceAccountId(subject.id)
      } catch (_: IllegalArgumentException) {
        rejectNotFound()
      } ?: rejectNotFound()

    if (
      (workloadDataplaneId != null && dataplane.id != workloadDataplaneId) ||
      dataplane.serviceAccountId != serviceAccountId ||
      dataplane.dataplaneGroupId != workloadDataplaneGroupId
    ) {
      rejectNotFound()
    }
    validateTopologyState(dataplane.enabled, dataplane.tombstone)

    val group = getAssignedDataplaneGroup(workloadDataplaneGroupId, organizationId)
    if (group.id != workloadDataplaneGroupId) {
      rejectNotFound()
    }
    if (group.organizationId != organizationId && group.organizationId != DEFAULT_ORGANIZATION_ID) {
      rejectNotFound()
    }
    validateTopologyState(group.enabled, group.tombstone)
    if (group.organizationId == DEFAULT_ORGANIZATION_ID) {
      rejectConflict()
    }
  }

  private fun getAssignedDataplaneGroup(
    dataplaneGroupId: UUID,
    workloadOrganizationId: UUID,
  ) = try {
    dataplaneGroupService.getDataplaneGroup(dataplaneGroupId, workloadOrganizationId)
  } catch (_: ConfigNotFoundException) {
    if (workloadOrganizationId == DEFAULT_ORGANIZATION_ID) {
      rejectNotFound()
    }
    try {
      dataplaneGroupService.getDataplaneGroup(dataplaneGroupId, DEFAULT_ORGANIZATION_ID)
    } catch (_: ConfigNotFoundException) {
      rejectNotFound()
    }
  }

  private fun validateTopologyState(
    enabled: Boolean?,
    tombstone: Boolean?,
  ) {
    if (enabled == null || tombstone == null) {
      rejectNotFound()
    }
    if (!enabled || tombstone) {
      throw ForbiddenException(UNAVAILABLE_MESSAGE)
    }
  }

  private fun validateLifecycle(workload: Workload) {
    if (workload.type !in FLEX_LOGGING_WORKLOAD_TYPES) {
      rejectConflict()
    }
    when (workload.status ?: rejectNotFound()) {
      WorkloadStatus.PENDING -> rejectConflict()
      WorkloadStatus.CLAIMED, WorkloadStatus.LAUNCHED, WorkloadStatus.RUNNING -> Unit
      WorkloadStatus.SUCCESS, WorkloadStatus.FAILURE, WorkloadStatus.CANCELLED ->
        throw InvalidStatusTransitionException(UNAVAILABLE_MESSAGE)
    }
  }

  private fun objectKeyPrefix(logPath: String): String {
    val normalizedLogPath = logPath.trim('/')
    if (normalizedLogPath.isBlank()) {
      rejectNotFound()
    }
    return "${DocumentType.LOGS.prefix}/$normalizedLogPath/"
  }

  private fun String.toUuidOrNotFound(): UUID =
    try {
      UUID.fromString(this)
    } catch (_: IllegalArgumentException) {
      rejectNotFound()
    }

  private fun rejectNotFound(): Nothing = throw NotFoundException(UNAVAILABLE_MESSAGE)

  private fun rejectConflict(): Nothing = throw ConflictException(UNAVAILABLE_MESSAGE)

  private companion object {
    const val UNAVAILABLE_MESSAGE = "Log upload authorization is unavailable."
    const val LOG_UPLOAD_AUTHORIZATION_ROUTE = "/api/v1/workload/{workloadId}/log-upload-authorization"
    const val OUTCOME_ISSUED = "issued"
    const val OUTCOME_DISABLED = "disabled"
    const val OUTCOME_REJECTED = "rejected"
    const val OUTCOME_BROKER_ERROR = "broker_error"
    const val NANOS_PER_MILLISECOND = 1_000_000.0
    val FLEX_LOGGING_WORKLOAD_TYPES = setOf(WorkloadType.SYNC, WorkloadType.CHECK, WorkloadType.DISCOVER)
  }
}
