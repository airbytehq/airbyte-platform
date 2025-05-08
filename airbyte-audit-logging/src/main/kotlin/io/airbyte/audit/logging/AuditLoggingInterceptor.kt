/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.audit.logging

import io.airbyte.audit.logging.model.Actor
import io.airbyte.audit.logging.model.AuditLogEntry
import io.airbyte.audit.logging.provider.AuditProvider
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.storage.AUDIT_LOGGING
import io.airbyte.commons.storage.AirbyteCloudStorageBulkUploader
import io.airbyte.commons.storage.DocumentType
import io.airbyte.commons.storage.StorageClientFactory
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.StoreAuditLogs
import io.airbyte.featureflag.Workspace
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Value
import io.micronaut.context.event.ShutdownEvent
import io.micronaut.context.event.StartupEvent
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.event.annotation.EventListener
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger { AUDIT_LOGGING }

/**
 * Interceptor that logs the requests and stores the log entries.
 */
@Singleton
@InterceptorBean(AuditLogging::class)
class AuditLoggingInterceptor(
  @Value("\${airbyte.audit.logging.enabled}") private val auditLoggingEnabled: Boolean,
  @Value("\${airbyte.cloud.storage.bucket.audit-logging:}") private val auditLoggingBucket: String?,
  private val applicationContext: ApplicationContext,
  private val auditLoggingHelper: AuditLoggingHelper,
  private val featureFlagClient: FeatureFlagClient,
  storageClientFactory: StorageClientFactory,
) : MethodInterceptor<Any, Any> {
  private val appender =
    AirbyteCloudStorageBulkUploader<AuditLogEntry>(
      AUDIT_LOGGING,
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
    val canStoreAuditLogs = featureFlagClient.boolVariation(StoreAuditLogs, Workspace(ANONYMOUS))

    // We can proceed with storing the audit log if:
    // 1. the AUDIT_LOGGING_ENABLED env var is set to true, or
    // 2. the StoreAuditLogs feature flag is enabled.
    // In cloud, the AUDIT_LOGGING_ENABLED var should never be set, thus we fall back to the flag. In enterprise
    // deployments, the feature flag is not present (and will default to false), so the env var must be
    // present in order to proceed.
    if (!auditLoggingEnabled && !canStoreAuditLogs) {
      logger.debug { "Proceeding with the request without audit logging because it is disabled." }
      return context.proceed() ?: Unit
    }

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
        logAuditInfo(
          actor = user,
          operationName = operationName,
          request = requestSummary,
          response = null,
          success = false,
          error = exception.message,
        )
        throw exception
      }

    val resultSummary = provider.get().generateSummaryFromResult(result)

    logAuditInfo(
      actor = user,
      operationName = operationName,
      request = requestSummary,
      response = resultSummary,
      success = true,
      error = null,
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
  ) {
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
      )

    val serializedAuditLogEntry = Jsons.serialize(auditLogEntry)
    if (auditLoggingBucket.isNullOrBlank()) {
      logger.info { "Audit logging storage bucket is not configured! Logging to console only: $serializedAuditLogEntry" }
      return
    }

    appender.append(auditLogEntry)
  }
}
