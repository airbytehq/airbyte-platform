package io.airbyte.audit.logging

import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.annotation.InternalForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.aop.InterceptorBean
import io.micronaut.aop.MethodInterceptor
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import jakarta.inject.Singleton
import java.util.UUID

// Todo: change this to a named logger
private val logger = KotlinLogging.logger {}

/**
 * Interceptor that logs the requests and stores the log entries.
 */
@Singleton
@InterceptorBean(AuditLogging::class)
class AuditLoggingInterceptor(
  @Value("\${airbyte.audit.logging.enabled}") private val auditLoggingEnabled: Boolean,
  private val applicationContext: ApplicationContext,
  private val auditLoggingHelper: AuditLoggingHelper,
) : MethodInterceptor<Any, Any> {
  override fun intercept(context: MethodInvocationContext<Any, Any>): Any {
    if (!auditLoggingEnabled) {
      logger.debug { "Proceed the request without audit logging because it is disabled." }
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
    val actionName = context.methodName
    logger.debug { "Audit logging the request, audit action: $actionName" }
    // Get request headers
    val request =
      ServerRequestContext.currentRequest<Any>().get() as NettyHttpRequest
    val headers = request.headers
    val user = getCurrentUserInfo(headers)
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
          user = user,
          actionName = actionName,
          summary = "",
          success = false,
          error = exception.message,
        )
        throw exception
      }
    val resultSummary = provider.get().generateSummaryFromResult(result)
    // 1. Merge the summary
    val summary = auditLoggingHelper.generateSummary(requestSummary, resultSummary)

    // 2. Save the log
    logAuditInfo(
      user = user,
      actionName = actionName,
      summary = summary,
      success = true,
      error = null,
    )
    return result ?: Unit
  }

  private fun getCurrentUserInfo(headers: HttpHeaders): User {
    val currentUser = auditLoggingHelper.getCurrentUser()
    val userAgent = headers.get("User-Agent")?.takeIf { it.isNotEmpty() } ?: "unknown"
    val ipAddress = headers.get("X-Forwarded-For")?.takeIf { it.isNotEmpty() } ?: "unknown"
    currentUser.userAgent = userAgent
    currentUser.ipAddress = ipAddress
    return currentUser
  }

  @InternalForTesting
  internal fun logAuditInfo(
    user: User,
    actionName: String,
    summary: String,
    success: Boolean,
    error: String? = null,
  ) {
    val auditLogEntry =
      AuditLogEntry(
        id = UUID.randomUUID(),
        timestamp = System.currentTimeMillis(),
        user = user,
        actionName = actionName,
        summary = summary,
        success = success,
        errorMessage = error,
      )
    logger.info { "Logging audit entry: $auditLogEntry" }
  }
}
