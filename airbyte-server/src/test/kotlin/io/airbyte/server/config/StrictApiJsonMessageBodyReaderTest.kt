/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import io.airbyte.server.apis.controllers.AttemptApiController
import io.airbyte.server.apis.controllers.ConnectionApiController
import io.airbyte.server.apis.controllers.DataplaneGroupApiController
import io.airbyte.server.apis.controllers.InstanceConfigurationApiController
import io.airbyte.server.apis.controllers.JobsApiController
import io.airbyte.server.apis.controllers.OrganizationApiController
import io.airbyte.server.apis.controllers.WebBackendApiController
import io.airbyte.server.apis.controllers.WorkspaceApiController
import io.micronaut.core.propagation.PropagatedContext
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.MediaType
import io.micronaut.http.codec.CodecException
import io.micronaut.http.context.ServerHttpRequestContext
import io.micronaut.jackson.databind.JacksonDatabindMapper
import io.micronaut.web.router.RouteAttributes
import io.micronaut.web.router.RouteInfo
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream

internal class StrictApiJsonMessageBodyReaderTest {
  private val objectMapper = ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
  private val flag = mockk<StrictJsonDeserializationFlag> { every { isEnabled() } returns true }
  private val reader =
    StrictApiJsonMessageBodyReader<TestRequestBody>(
      JacksonDatabindMapper(objectMapper),
      objectMapper,
      flag,
    )

  @Test
  fun `scim base path stays lenient for an allowlisted controller`() {
    assertDoesNotThrow { readUnknownProperty("/scim/v2") }
  }

  @Test
  fun `scim child path stays lenient for an allowlisted controller`() {
    assertDoesNotThrow { readUnknownProperty("/scim/v2/Users") }
  }

  @Test
  fun `similar non scim path stays strict for an allowlisted controller`() {
    assertThrows(Exception::class.java) { readUnknownProperty("/scim/v20") }
  }

  @Test
  fun `attempt controller rejects unknown properties`() {
    val exception =
      assertThrows(CodecException::class.java) {
        readUnknownProperty("/api/v1/attempt/save_stats", AttemptApiController::class.java)
      }

    val cause = assertInstanceOf(UnrecognizedPropertyException::class.java, exception.cause)
    assertEquals("unknown", cause.propertyName)
  }

  @Test
  fun `jobs controller rejects unknown properties`() {
    val exception =
      assertThrows(CodecException::class.java) {
        readUnknownProperty("/api/v1/jobs/get", JobsApiController::class.java)
      }

    val cause = assertInstanceOf(UnrecognizedPropertyException::class.java, exception.cause)
    assertEquals("unknown", cause.propertyName)
  }

  @Test
  fun `only explicitly allowlisted controllers use strict deserialization`() {
    listOf(
      OrganizationApiController::class.java,
      WorkspaceApiController::class.java,
      ConnectionApiController::class.java,
      WebBackendApiController::class.java,
      DataplaneGroupApiController::class.java,
    ).forEach { controller ->
      assertThrows(Exception::class.java) { readUnknownProperty("/api/v1/test", controller) }
    }

    assertDoesNotThrow { readUnknownProperty("/api/v1/test", InstanceConfigurationApiController::class.java) }
  }

  private fun readUnknownProperty(
    path: String,
    declaringType: Class<*> = OrganizationApiController::class.java,
  ): TestRequestBody {
    val request = HttpRequest.POST(path, "")
    val routeInfo = mockk<RouteInfo<Any>>()
    every { routeInfo.declaringType } returns declaringType
    RouteAttributes.setRouteInfo(request, routeInfo)

    return PropagatedContext
      .getOrEmpty()
      .plus(ServerHttpRequestContext(request))
      .propagate()
      .use {
        reader.read(
          Argument.of(TestRequestBody::class.java),
          MediaType.APPLICATION_JSON_TYPE,
          request.headers,
          ByteArrayInputStream("""{"declared":"value","unknown":"value"}""".toByteArray()),
        )
      }
  }

  class TestRequestBody {
    var declared: String? = null
  }
}
