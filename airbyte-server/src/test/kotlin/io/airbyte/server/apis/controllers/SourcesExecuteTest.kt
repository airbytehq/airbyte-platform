/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.micronaut.context.ApplicationContext
import io.micronaut.inject.BeanConfiguration
import io.micronaut.inject.BeanDefinitionReference
import io.micronaut.runtime.server.EmbeddedServer
import io.micronaut.security.filters.SecurityFilter
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID

class SourcesExecuteTest {
  @Test
  fun `OSS config API retains source routes but exposes no execute endpoint`() {
    val sourceId = UUID.randomUUID()
    val sourceHandler = mockk<SourceHandler>()
    every { sourceHandler.getSource(any()) } returns SourceRead().sourceId(sourceId).name("Existing source")
    val isolated =
      object : BeanConfiguration by BeanConfiguration.disabled("io.airbyte") {
        override fun isWithin(beanDefinitionReference: BeanDefinitionReference<*>): Boolean =
          isWithin(beanDefinitionReference.beanDefinitionName.replace("$", ""))

        override fun isWithin(cls: Class<*>): Boolean = isWithin(cls.name)

        override fun isWithin(className: String): Boolean =
          className.startsWith("io.airbyte.") && !className.startsWith("io.airbyte.server.apis.controllers.SourceApiController")
      }
    ApplicationContext
      .builder()
      .enableDefaultPropertySources(false)
      .deduceEnvironment(false)
      .environmentPropertySource(false)
      .beanConfigurations(isolated)
      .properties(
        mapOf(
          "micronaut.server.port" to -1,
          "micronaut.security.enabled" to false,
          "micronaut.security.filter.enabled" to false,
          "micronaut.executors.io.type" to "cached",
        ),
      ).build()
      .use { context ->
        context.registerSingleton(SchedulerHandler::class.java, mockk<SchedulerHandler>())
        context.registerSingleton(SourceHandler::class.java, sourceHandler)
        context.start()
        assertFalse(context.containsBean(SecurityFilter::class.java))
        context.getBean(EmbeddedServer::class.java).start().use { server ->
          HttpClient.newHttpClient().use { client ->
            val get =
              HttpRequest
                .newBuilder(URI("${server.url}/api/v1/sources/get"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"sourceId":"$sourceId"}"""))
                .build()
            val response = client.send(get, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, response.statusCode())
            assertEquals(sourceId.toString(), ObjectMapper().readTree(response.body()).get("sourceId").asText())
            val execute =
              HttpRequest
                .newBuilder(URI("${server.url}/api/v1/sources/$sourceId/execute"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""{"entity":"issues","action":"list"}"""))
                .build()
            assertEquals(404, client.send(execute, HttpResponse.BodyHandlers.ofString()).statusCode())
          }
        }
      }
    verify(exactly = 1) { sourceHandler.getSource(any()) }
  }
}
