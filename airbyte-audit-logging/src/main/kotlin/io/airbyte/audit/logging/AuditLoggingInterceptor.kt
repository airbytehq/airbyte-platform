/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.audit.logging.provider.AuditProvider
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.entitlements.EntitlementService
import io.airbyte.commons.entitlements.models.AuditLoggingEntitlement
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.domain.models.OrganizationId
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.StoreAuditLogs
import io.airbyte.micronaut.runtime.AirbyteStorageConfig
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.context.event.StartupEvent
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.inject.Singleton
import kotlinx.coroutines.runBlocking
import java.util.UUID

/**
 * Interceptor that logs the requests and stores the log entries.
 */
@Singleton
@InterceptorBean(AuditLogging::class)
class AuditLoggingInterceptor(
  private val applicationContext: ApplicationContext,
  private val auditLoggingHelper: AuditLoggingHelper,
  private val auditScopeResolver: AuditScopeResolver,
  private val entitlementService: EntitlementService,
  private val featureFlagClient: FeatureFlagClient,
  private val storageConfiguration: AirbyteStorageConfig,
  storageClientFactory: StorageClientFactory,
) : MethodInterceptor<Any, Any> {
  private val logger: KLogger = KotlinLogging.logger { storageConfiguration.bucket.auditLogging }

  private val appender =
    AuditLogBulkUploader(
      storageConfiguration.bucket.auditLogging,
      storageClientFactory.create(DocumentType.AUDIT_LOGS),
    )

  @EventListener
  fun onStartupEvent(event: StartupEvent) {
    appender.start()
  }

  @EventListener
  fun onShutdownEvent(event: ShutdownEvent) {
    appender.stop()
  }

  override fun intercept(context: MethodInvocationContext<Any, Any>): Any {
    val annotation = context.getAnnotation(AuditLogging::class.java)
    if (annotation == null) {
      logger.error { "Failed to retrieve the audit logging annotation." }
      return context.proceed() ?: Unit
    }

    val providerName = annotation.stringValue("provider")
    if (providerName == null || providerName.isEmpty) {
      logger.error { "Provider name is missing. Bypassing audit logging." }
      return context.proceed() ?: Unit
    }

    val provider = applicationContext.findBean(AuditProvider::class.java, Qualifiers.byName(providerName.get()))
    if (provider.isEmpty) {
      logger.error { "Failed to retrieve the audit provider. Bypassing audit logging." }
      return context.proceed() ?: Unit
    }

    // Get action name
    val operationName = context.methodName
    logger.debug { "Audit logging the request, audit action: $operationName" }

    // Get request headers
    val request =
      ServerRequestContext.currentRequest<Any>().get() as NettyHttpRequest
    val headers = request.headers
    val user = auditLoggingHelper.buildActor(headers)
    if (user == null) {
      logger.debug { "Skipping audit log for $operationName: no actor could be resolved for the request." }
      return context.proceed() ?: Unit
    }

    // Get request body
    val parameters = context.parameters.values
    val requestBody = parameters.firstOrNull()?.value

    // Generate the summary from the request, before proceeding the request
    val requestSummary = provider.get().generateSummaryFromRequest(requestBody)

    // Proceed the request and log the result/error
    val result =
      try {
        context.proceed()
      } catch (exception: Exception) {
        val failureScope = auditScopeResolver.resolveScope(headers, requestBody, null)
        logAuditInfo(
          actor = user,
          operationName = operationName,
          request = requestSummary,
          response = null,
          success = false,
          error = exception.message,
          organizationId = failureScope.organizationId,
          workspaceId = failureScope.workspaceId,
        )
        throw exception
      }

    val resultSummary = provider.get().generateSummaryFromResult(result)

    val scope = auditScopeResolver.resolveScope(headers, requestBody, result)
    logAuditInfo(
      actor = user,
      operationName = operationName,
      request = requestSummary,
      response = resultSummary,
      success = true,
      error = null,
      organizationId = scope.organizationId,
      workspaceId = scope.workspaceId,
    )

    return result ?: Unit
  }

  @InternalForTesting
  internal fun logAuditInfo(
    actor: Actor,
    operationName: String,
    request: Any?,
    response: Any?,
    success: Boolean,
    error: String? = null,
    organizationId: UUID? = null,
    workspaceId: UUID? = null,
  ) {
    if (organizationId == null) {
      logger.debug { "Skipping audit log for $operationName: no organization could be resolved for the request." }
      return
    }
    // Storing audit logs requires both the StoreAuditLogs feature flag (the rollout lever, keyed on
    // the organization so it can be enabled per org) and the audit-logging entitlement, both
    // evaluated against the organization the entry is attributed to.
    if (!featureFlagClient.boolVariation(StoreAuditLogs, Organization(organizationId))) {
      logger.debug { "Skipping audit log for $operationName: audit logging is disabled for organization $organizationId." }
      return
    }
    if (!entitlementService.checkEntitlement(OrganizationId(organizationId), AuditLoggingEntitlement).isEntitled) {
      logger.debug { "Skipping audit log for $operationName: organization $organizationId is not entitled to audit logs." }
      return
    }

    val auditLogEntry =
      AuditLogEntry(
        id = UUID.randomUUID(),
        timestamp = System.currentTimeMillis(),
        actor = actor,
        operation = operationName,
        request = request,
        response = response,
        success = success,
        errorMessage = error,
        organizationId = organizationId,
        workspaceId = workspaceId,
      )

    val serializedAuditLogEntry = Jsons.serialize(auditLogEntry)
    if (storageConfiguration.bucket.auditLogging.isBlank()) {
      logger.info { "Audit logging storage bucket is not configured! Logging to console only: $serializedAuditLogEntry" }
      return
    }

    runBlocking {
      appender.append(auditLogEntry)
    }
  }
}
