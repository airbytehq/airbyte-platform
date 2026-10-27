/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.data.auth.TokenType
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.inject.qualifiers.Qualifiers
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.micronaut.web.router.Router
import io.mockk.Called
import io.mockk.clearMocks
import io.mockk.mockkClass
import io.mockk.verify
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.reactivestreams.Publisher
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID

/** Shared server plumbing only. Each family supplies its own explicit policy and business fixtures. */
class RemainingOperationHttpSupport(
  private val cloud: Boolean,
  private val controllers: List<Class<*>>,
  private val realRules: Set<String>,
  private val security: Boolean = true,
  private val earlierAllow: Boolean = true,
) {
  fun run(
    configure: (ApplicationContext, List<Any>) -> Unit,
    check: (ApplicationContext, EmbeddedServer, List<Any>) -> Unit,
  ) {
    val allowed =
      controllers.map { it.name } + realRules +
        listOf(
          "io.airbyte.server.auth.RemainingOperationAuthentication",
          "io.airbyte.server.auth.RemainingOperationEarlierAllow",
          "io.airbyte.server.auth.AuthorizedRequest",
          "io.airbyte.server.auth.AuthorizationInterceptor",
          "io.airbyte.server.auth.RouteAccess",
          "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
          "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
          "io.airbyte.commons.server.errors.handlers.KnownExceptionHandler",
        )
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

        override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

        override fun isWithin(name: String): Boolean =
          name.startsWith("io.airbyte.") &&
            allowed.filter { earlierAllow || it != "io.airbyte.server.auth.RemainingOperationEarlierAllow" }.none(name::startsWith)
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .environments(if (cloud) "cloud" else "test")
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "spec.name" to "RemainingOperationHttpTest",
          "micronaut.security.enabled" to security,
          "micronaut.security.filter.enabled" to security,
          "micronaut.server.port" to -1,
          "micronaut.server.host" to "127.0.0.1",
          "micronaut.metrics.enabled" to false,
          "micronaut.executors.io.type" to "cached",
          "micronaut.executors.scheduler.type" to "cached",
          "micronaut.executors.webhook.type" to "cached",
          "micronaut.executors.sonar-forwarding.type" to "cached",
        ),
      ).build()
      .use { context ->
        val business = mutableListOf<Any>()
        for (parameter in controllers
          .flatMap {
            it.declaredConstructors
              .single { !it.isSynthetic }
              .parameters
              .toList()
          }.distinctBy { it.type to it.getAnnotation(Named::class.java)?.value }) {
          @Suppress("UNCHECKED_CAST")
          val type = parameter.type as Class<Any>
          val instance =
            when (type.name) {
              "com.fasterxml.jackson.databind.ObjectMapper" -> MoreMappers.initMapper()
              "io.airbyte.server.wrapped.apis.ApiHelper" -> type.getConstructor().newInstance()
              else -> mockkClass(type.kotlin).also(business::add)
            }
          val name = parameter.getAnnotation(Named::class.java)?.value
          if (name == null) context.registerSingleton(type, instance) else context.registerSingleton(type, instance, Qualifiers.byName(name))
        }
        configure(context, business)
        val selected =
          controllers
            .flatMap { it.methods.toList() }
            .mapNotNull { it.getAnnotation(AuthorizeOperation::class.java) }
            .map { it.rule.java }
            .filter { it != OperationRule::class.java && it.name in realRules }
            .toSet()
        for (controller in controllers) registerSiblingOperationRules(context, controller, selected)
        context.start()
        business.forEach { clearMocks(it, answers = false, recordedCalls = true) }
        context.getBean(EmbeddedServer::class.java).start().use { server -> check(context, server, business) }
      }
  }

  companion object {
    fun request(
      server: EmbeddedServer,
      method: String,
      path: String,
      body: String,
      token: String,
    ): HttpResponse<String> =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build().use { client ->
        client.send(
          java.net.http.HttpRequest
            .newBuilder(URI("${server.url}$path"))
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer $token")
            .method(
              method,
              java.net.http.HttpRequest.BodyPublishers
                .ofString(body),
            ).build(),
          HttpResponse.BodyHandlers.ofString(),
        )
      }

    fun requireRule(
      context: ApplicationContext,
      method: String,
      template: String,
      expected: String,
    ) {
      val route =
        context
          .getBean(Router::class.java)
          .uriRoutes()
          .filter {
            it.httpMethodName == method && it.uriMatchTemplate.toString().replace(Regex("\\{[?&][^}]*}"), "") == template
          }.findFirst()
          .orElseThrow()
      assertEquals(
        expected,
        route.annotationMetadata
          .classValue(AuthorizeOperation::class.java, "rule")
          .orElseThrow()
          .name,
        "$method $template",
      )
    }

    fun requireNoBusiness(business: List<Any>) {
      business.forEach { verify { it wasNot Called } }
    }
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingOperationHttpTest")
class RemainingOperationAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> {
    val token =
      request.headers.authorization
        .orElse("")
        .removePrefix("Bearer ")
    val attributes =
      when {
        token == "user" || token == "application-owner" -> emptyMap()
        token == "internal" -> mapOf(TokenType.INTERNAL_CLIENT.toClaim())
        token == "dataplane" -> mapOf(TokenType.DATAPLANE_V1.toClaim())
        token == "service" -> mapOf(TokenType.SERVICE_ACCOUNT.toClaim())
        token == "embedded" -> mapOf(TokenType.EMBEDDED_V1.toClaim())
        token.startsWith("embedded-scoped:") ->
          mapOf(
            TokenType.EMBEDDED_V1.toClaim(),
            TokenScopeClaim.CLAIM_ID to TokenScopeClaim(token.removePrefix("embedded-scoped:")),
          )
        token == "workload" -> mapOf(TokenType.WORKLOAD_API.toClaim())
        token.startsWith("scoped:") -> mapOf(TokenScopeClaim.CLAIM_ID to TokenScopeClaim(token.removePrefix("scoped:")))
        else -> return Publishers.empty()
      }
    val subject = if (token == "dataplane" || token == "service") UUID(0, 9).toString() else "caller"
    return Publishers.just(Authentication.build(subject, listOf(AuthRoleConstants.ADMIN), attributes))
  }
}

@Singleton
@Requires(property = "spec.name", value = "RemainingOperationHttpTest")
class RemainingOperationEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}
