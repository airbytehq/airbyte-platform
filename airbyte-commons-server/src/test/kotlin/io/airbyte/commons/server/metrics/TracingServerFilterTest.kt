/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.commons.server.support.AuthenticationHeaderResolver
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.metrics.MetricClient
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.filter.ServerFilterChain
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import reactor.core.publisher.Mono
import java.util.UUID

class TracingServerFilterTest {
  @Test
  fun `trace uses request body instead of client reference header`() {
    val workspaceId = UUID.randomUUID()
    val resolver = mockk<AuthenticationHeaderResolver>()
    every { resolver.resolveOrganization(mapOf(AuthenticationId.WORKSPACE_ID.httpHeader to workspaceId.toString())) } returns null
    val registry = SimpleMeterRegistry()
    val filter = TracingServerFilter(resolver, MetricClient(registry))
    val request =
      HttpRequest
        .POST("/api/v1/sources/list", mapOf("workspaceId" to workspaceId.toString()))
        .header(AuthenticationId.WORKSPACE_ID.httpHeader, UUID.randomUUID().toString())

    filter.traceRequest(request)
    filter.traceResponse(request, HttpResponse.ok<Any>(), null)

    assertEquals(
      workspaceId.toString(),
      registry
        .find("api-trace")
        .timers()
        .single()
        .id
        .getTag("refs.workspaceId"),
    )
  }

  @Test
  fun `records request metrics when workspace resolution fails`() {
    val organizationId = UUID.randomUUID()
    val sourceId = UUID.randomUUID()
    val resolver = mockk<AuthenticationHeaderResolver>()
    every {
      resolver.resolveWorkspace(
        mapOf(
          AuthenticationId.ORGANIZATION_ID.httpHeader to organizationId.toString(),
          AuthenticationId.SOURCE_ID.httpHeader to sourceId.toString(),
        ),
      )
    } throws IllegalStateException("Workspace lookup failed")
    val registry = SimpleMeterRegistry()
    val filter = TracingServerFilter(resolver, MetricClient(registry))
    val request = HttpRequest.POST("/api/v1/sources/get", mapOf("organizationId" to organizationId.toString(), "sourceId" to sourceId.toString()))
    val response = HttpResponse.ok("response body").header("X-Response", "preserved")
    val chain = mockk<ServerFilterChain> { every { proceed(request) } returns Mono.just(response) }

    assertSame(response, Mono.from(filter.doFilter(request, chain)).block())

    val timers = registry.find("api-trace").timers()
    assertEquals(1L, timers.sumOf { it.count() }, "$timers")
    assertEquals(organizationId.toString(), timers.single().id.getTag("refs.organizationId"))
  }

  @Test
  fun `records request metrics when organization resolution fails`() {
    val sourceId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val properties = mapOf(AuthenticationId.SOURCE_ID.httpHeader to sourceId.toString())
    val resolver = mockk<AuthenticationHeaderResolver>()
    every { resolver.resolveWorkspace(properties) } returns listOf(workspaceId)
    every { resolver.resolveOrganization(properties) } throws IllegalStateException("Organization lookup failed")
    val registry = SimpleMeterRegistry()
    val filter = TracingServerFilter(resolver, MetricClient(registry))
    val request = HttpRequest.POST("/api/v1/sources/get", mapOf("sourceId" to sourceId.toString()))
    val response = HttpResponse.ok("response body").header("X-Response", "preserved")
    val chain = mockk<ServerFilterChain> { every { proceed(request) } returns Mono.just(response) }

    assertSame(response, Mono.from(filter.doFilter(request, chain)).block())

    val timers = registry.find("api-trace").timers()
    assertEquals(1L, timers.sumOf { it.count() }, "$timers")
    assertEquals(workspaceId.toString(), timers.single().id.getTag("refs.workspaceId"))
  }
}
