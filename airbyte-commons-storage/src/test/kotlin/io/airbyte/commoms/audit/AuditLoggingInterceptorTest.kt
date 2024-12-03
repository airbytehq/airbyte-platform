package io.airbyte.commoms.audit

import io.airbyte.api.model.generated.PermissionRead
import io.airbyte.api.model.generated.PermissionType
import io.airbyte.commons.annotation.AuditLogging
import io.airbyte.commons.audit.AuditLoggingInterceptor
import io.airbyte.commons.audit.AuditProvider
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.ApplicationContext
import io.micronaut.core.annotation.AnnotationValue
import io.micronaut.http.HttpHeaders
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.http.server.netty.NettyHttpRequest
import io.micronaut.inject.qualifiers.Qualifiers
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkAll
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.Optional
import java.util.UUID

class AuditLoggingInterceptorTest {
  private lateinit var interceptor: AuditLoggingInterceptor
  private lateinit var context: MethodInvocationContext<Any, Any>
  private lateinit var applicationContext: ApplicationContext

  @BeforeEach
  fun setUp() {
    context = mockk()
    applicationContext = mockk()
  }

  @AfterEach
  fun tearDown() {
    unmockkAll()
  }

  @Test
  fun `should only proceed the request without logging the result if it is not enabled`() {
    interceptor = AuditLoggingInterceptor(false, applicationContext)

    every { context.methodName } returns "createPermission"

    every { context.proceed() } returns
      PermissionRead()
        .userId(UUID.randomUUID())
        .workspaceId(UUID.randomUUID())
        .organizationId(null)
        .permissionType(PermissionType.WORKSPACE_EDITOR)

    interceptor.intercept(context)

    verify { context.proceed() }
  }

  @Test
  fun `should proceed the request and log the result`() {
    interceptor = spyk(AuditLoggingInterceptor(true, applicationContext))
    val request = mockk<NettyHttpRequest<Any>>()
    val headers = mockk<HttpHeaders>()

    val actionName = "createPermission"
    every { context.methodName } returns actionName
    every { request.headers } returns headers
    every { headers.get("X-Airbyte-User-Id") } returns "userId"
    every { headers.get("User-Agent") } returns "userAgent"
    every { headers.get("X-Forwarded-For") } returns null

    mockkStatic(ServerRequestContext::class)
    every { ServerRequestContext.currentRequest<Any>() } returns Optional.of(request)

    // Mock the audit logging annotation
    val auditLoggingAnnotation = mockk<AnnotationValue<AuditLogging>>()
    every { context.getAnnotation(AuditLogging::class.java) } returns auditLoggingAnnotation
    every { auditLoggingAnnotation.stringValue("provider") } returns Optional.of("testProvider")

    // Mock the application context to return a fake provider
    val fakeProvider = mockk<AuditProvider>()
    every { fakeProvider.generateSummary(any()) } returns "summary"
    every { applicationContext.findBean(AuditProvider::class.java, Qualifiers.byName("testProvider")) } returns Optional.of(fakeProvider)

    val targetUserId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    every { context.proceed() } returns
      PermissionRead()
        .userId(targetUserId)
        .workspaceId(workspaceId)
        .organizationId(null)
        .permissionType(PermissionType.WORKSPACE_EDITOR)

    interceptor.intercept(context)
    // Verifying that request is proceeded
    verify { context.proceed() }
    // Verify logAuditInfo was called with the correct parameters
    verify {
      interceptor.logAuditInfo(
        user = match { it.userId == "userId" && it.userAgent == "userAgent" },
        actionName = "createPermission",
        summary = "summary",
        success = true,
        error = null,
      )
    }
  }
}
