package io.airbyte.commons.audit

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
) : MethodInterceptor<Any, Any> {
  override fun intercept(context: MethodInvocationContext<Any, Any>): Any {
    if (!auditLoggingEnabled) {
      logger.debug { "Proceed the request without audit logging because it is disabled." }
      return context.proceed()
    }

    val annotation = context.getAnnotation(AuditLogging::class.java)
    if (annotation == null) {
      logger.error { "Failed to retrieve the audit logging annotation." }
      return context.proceed()
    }

    val providerName = annotation.stringValue("provider")
    if (providerName == null || providerName.isEmpty) {
      logger.error { "Provider name is missing. Bypassing audit logging." }
      return context.proceed()
    }

    val provider = applicationContext.findBean(AuditProvider::class.java, Qualifiers.byName(providerName.get()))
    if (provider.isEmpty) {
      logger.error { "Failed to retrieve the audit provider. Bypassing audit logging." }
      return context.proceed()
    }

    val actionName = context.methodName
    logger.debug { "Audit logging the request, audit action: $actionName" }
    val request =
      ServerRequestContext.currentRequest<Any>().get() as NettyHttpRequest
    val headers = request.headers
    val user = getCurrentUserInfo(headers)

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
    // 1. Generate the summary depends on the provider
    val summary = provider.get().generateSummary(result)
    // 2. Save the log
    logAuditInfo(
      user = user,
      actionName = actionName,
      summary = summary,
      success = true,
      error = null,
    )
    return result
  }

  private fun getCurrentUserInfo(headers: HttpHeaders): User {
    val userId = headers.get("X-Airbyte-User-Id")?.takeIf { it.isNotEmpty() } ?: "unknown"
    val userAgent = headers.get("User-Agent")?.takeIf { it.isNotEmpty() } ?: "unknown"
    val ipAddress = headers.get("X-Forwarded-For")?.takeIf { it.isNotEmpty() } ?: "unknown"
    // TODO: Retrieve user email
    val user =
      User(
        userId = userId,
        userAgent = userAgent,
        ipAddress = ipAddress,
      )
    return user
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
