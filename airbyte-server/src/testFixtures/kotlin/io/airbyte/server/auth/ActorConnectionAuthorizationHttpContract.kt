/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.OperationRule
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Requires
import io.micronaut.core.async.publisher.Publishers
import io.micronaut.core.io.service.SoftServiceLoader
import io.micronaut.http.HttpRequest
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.authentication.Authentication
import io.micronaut.security.filters.AuthenticationFetcher
import io.micronaut.security.rules.SecurityRule
import io.micronaut.security.rules.SecurityRuleResult
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkClass
import io.mockk.verify
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.UUID

/** Real production controllers and advice; external storage and business dependencies are isolated. */
abstract class ActorConnectionAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `a legacy role label cannot approve a source without saved permission even after earlier ALLOWED`() {
    request(status = 403)
  }

  @Test
  fun `required saved source lookup failures prevent business work`() {
    request(status = 404, missing = true)
    request(status = 500, failure = true)
  }

  @Test
  fun `malformed missing and null source identifiers stop before business work`() {
    for (body in listOf("{}", "null", "", "{", """{"sourceId":null}""", """{"sourceId":"bad"}""")) request(status = 400, body = body)
  }

  @Test
  fun `a saved reader grant reaches normal source business work and response in both editions`() {
    request(status = 200, granted = true)
  }

  private fun request(
    status: Int,
    body: String = """{"sourceId":"$SOURCE"}""",
    missing: Boolean = false,
    failure: Boolean = false,
    granted: Boolean = false,
  ) {
    val controller =
      if (cloud) {
        Class.forName("io.airbyte.server.wrapped.apis.SourceApiWrappedController")
      } else {
        io.airbyte.server.apis.controllers.SourceApiController::class.java
      }
    val rules =
      SoftServiceLoader
        .load(BeanDefinitionReference::class.java)
        .collectAll()
        .filter { OperationRule::class.java.isAssignableFrom(it.beanType) }
        .map { it.beanType.name }
    val allowed =
      rules +
        listOf(
          controller.name,
          "io.airbyte.server.apis.controllers.SourceApiController",
          "io.airbyte.server.auth.AuthorizedRequest",
          "io.airbyte.server.auth.AuthorizationInterceptor",
          "io.airbyte.server.auth.RouteAccess",
          "io.airbyte.server.auth.ActorConnectionAuthentication",
          "io.airbyte.server.auth.ActorConnectionEarlierAllow",
          "io.airbyte.commons.server.errors.problems.AbstractThrowableProblemHandler",
          "io.airbyte.commons.server.errors.handlers.UncaughtExceptionHandler",
        )
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(reference: BeanDefinitionReference<*>): Boolean = isWithin(reference.beanDefinitionName.replace("$", ""))

        override fun isWithin(type: Class<*>): Boolean = isWithin(type.name)

        override fun isWithin(name: String): Boolean = name.startsWith("io.airbyte.") && allowed.none(name::startsWith)
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
          "spec.name" to "ActorConnectionAuthorizationHttpTest",
          "micronaut.security.enabled" to true,
          "micronaut.security.filter.enabled" to true,
          "micronaut.server.port" to -1,
          "micronaut.metrics.enabled" to false,
        ),
      ).build()
      .use { context ->
        val data = mockk<AuthorizationDataService>(relaxed = true)
        every { data.savedSourceWorkspaceAndOrganization(SOURCE) } returns WorkspaceAndOrganization(WORKSPACE, ORGANIZATION)
        if (missing) every { data.savedSourceWorkspaceAndOrganization(SOURCE) } throws AuthorizationResourceNotFoundException()
        if (failure) every { data.savedSourceWorkspaceAndOrganization(SOURCE) } throws IOException("ownership unavailable")
        val permissions = mockk<PermissionService>()
        every { permissions.getPermissionsByAuthUserId(any()) } returns
          if (granted) listOf(Permission().withWorkspaceId(WORKSPACE).withPermissionType(Permission.PermissionType.WORKSPACE_READER)) else emptyList()
        for (parameter in controller.declaredConstructors.single { !it.isSynthetic }.parameters) {
          @Suppress("UNCHECKED_CAST")
          val type = parameter.type as Class<Any>
          val instance =
            if (type == ObjectMapper::class.java) {
              MoreMappers.initMapper()
            } else if (type.name == "io.airbyte.server.wrapped.apis.ApiHelper") {
              type.getConstructor().newInstance()
            } else {
              mockkClass(type.kotlin, relaxed = true)
            }
          if (type.simpleName == "SpecMutatingHandler") {
            val method = type.getMethod("getSource", SourceIdRequestBody::class.java)
            every { method.invoke(instance, any<SourceIdRequestBody>()) } returns sourceRead()
          }
          context.registerSingleton(type, instance)
        }
        val sourceHandler = context.getBean(SourceHandler::class.java)
        every { sourceHandler.getSource(any<SourceIdRequestBody>()) } returns sourceRead()
        context.registerSingleton(AuthorizationDataService::class.java, data)
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(permissions, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
        context.start()
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val response =
              client.send(
                java.net.http.HttpRequest
                  .newBuilder(URI("${server.url}/api/v1/sources/get"))
                  .header("Content-Type", "application/json")
                  .header("Authorization", "Bearer test")
                  .POST(
                    java.net.http.HttpRequest.BodyPublishers
                      .ofString(body),
                  ).build(),
                HttpResponse.BodyHandlers.ofString(),
              )
            assertEquals(status, response.statusCode(), response.body())
            if (status == 200) {
              assertEquals(
                SOURCE.toString(),
                io.airbyte.commons.json.Jsons
                  .deserialize(response.body())["sourceId"]
                  .asText(),
              )
              if (!cloud) verify(exactly = 1) { sourceHandler.getSource(any<SourceIdRequestBody>()) }
            } else {
              verify(exactly = 0) { sourceHandler.getSource(any<SourceIdRequestBody>()) }
            }
          }
        }
      }
  }

  companion object {
    val SOURCE: UUID = UUID(0, 101)
    val WORKSPACE: UUID = UUID(0, 102)
    val ORGANIZATION: UUID = UUID(0, 103)

    private fun sourceRead(): SourceRead =
      SourceRead(
        sourceDefinitionId = UUID(0, 104),
        sourceId = SOURCE,
        workspaceId = WORKSPACE,
        connectionConfiguration = Jsons.emptyObject(),
        name = "source",
        sourceName = "source-definition",
        createdAt = 0L,
      )
  }
}

@Singleton
@Requires(property = "spec.name", value = "ActorConnectionAuthorizationHttpTest")
class ActorConnectionAuthentication : AuthenticationFetcher<HttpRequest<*>> {
  override fun fetchAuthentication(request: HttpRequest<*>): Publisher<Authentication> =
    Publishers.just(Authentication.build("caller", listOf(AuthRoleConstants.WORKSPACE_READER)))
}

@Singleton
@Requires(property = "spec.name", value = "ActorConnectionAuthorizationHttpTest")
class ActorConnectionEarlierAllow : SecurityRule<HttpRequest<*>> {
  override fun getOrder(): Int = Int.MIN_VALUE

  override fun check(
    request: HttpRequest<*>,
    authentication: Authentication?,
  ): Publisher<SecurityRuleResult> = Publishers.just(SecurityRuleResult.ALLOWED)
}
