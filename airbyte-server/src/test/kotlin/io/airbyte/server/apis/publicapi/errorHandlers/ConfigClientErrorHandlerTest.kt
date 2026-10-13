/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.errorHandlers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.InvalidApiKeyProblem
import io.airbyte.api.problems.throwable.generated.OAuthCallbackFailureProblem
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.api.problems.throwable.generated.TryAgainLaterConflictProblem
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.ConflictException
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.publicApi.server.generated.models.ConnectionCreateRequest
import io.airbyte.server.apis.publicapi.exceptions.OAuthCallbackException
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpResponseFactory
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.util.UUID

class ConfigClientErrorHandlerTest {
  private val resourceId = UUID.randomUUID()

  private val httpResponseFactory = HttpResponseFactory.INSTANCE

  @Test
  fun `test that it can handle errors for an HttpResponse`() {
    val notFoundResponse = HttpResponse.notFound("body")
    assertThrows<ResourceNotFoundProblem> { ConfigClientErrorHandler.handleError(notFoundResponse, resourceId.toString()) }

    val conflictResponse =
      httpResponseFactory
        .status<String>(HttpStatus.CONFLICT, "test")
        .body(mapOf("message" to "test"))
    assertThrows<TryAgainLaterConflictProblem> { ConfigClientErrorHandler.handleError(conflictResponse, resourceId.toString()) }

    val unauthorizedResponse = httpResponseFactory.status<String>(HttpStatus.UNAUTHORIZED)
    assertThrows<InvalidApiKeyProblem> { ConfigClientErrorHandler.handleError(unauthorizedResponse, resourceId.toString()) }

    val unprocessibleEntityResponse =
      httpResponseFactory
        .status<String>(HttpStatus.UNPROCESSABLE_ENTITY, "test")
        .body(mapOf("message" to "test"))
    assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(unprocessibleEntityResponse, resourceId.toString()) }

    val badRequestResponse = httpResponseFactory.status<String>(HttpStatus.BAD_REQUEST, "test")
    assertThrows<BadRequestProblem> { ConfigClientErrorHandler.handleError(badRequestResponse, resourceId.toString()) }

    val unexpectedResponse = httpResponseFactory.status<String>(HttpStatus.INTERNAL_SERVER_ERROR, "test")
    assertThrows<UnexpectedProblem> { ConfigClientErrorHandler.handleError(unexpectedResponse, resourceId.toString()) }
  }

  @Test
  fun `test that it can handle throwables`() {
    assertThrows<ResourceNotFoundProblem> { ConfigClientErrorHandler.handleError(ConfigNotFoundException("test", "test")) }

    assertThrows<TryAgainLaterConflictProblem> { ConfigClientErrorHandler.handleError(ValueConflictKnownException("test")) }

    val conflict = assertThrows<StateConflictProblem> { ConfigClientErrorHandler.handleError(ConflictException("test")) }
    assert(conflict.problem.getStatus() == 409)

    assertThrows<StateConflictProblem> { ConfigClientErrorHandler.handleError(IllegalStateException()) }

    assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(JsonValidationException("test")) }
  }

  @Test
  fun `test that it can handle JSON validation errors gracefully`() {
    val schema =
      Jsons.deserialize(
        """
        {
            "type": "object",
            "title": "Pokeapi Spec",
            "${"$"}schema": "http://json-schema.org/draft-07/schema#",
            "required": [
                "pokemon_name"
            ],
            "properties": {
                "pokemon_name": {
                    "enum": [
                        "bulbasaur",
                        "ivysaur"
                    ],
                    "type": "string",
                    "title": "Pokemon Name"
                }
            }
        }            
        """.trimIndent(),
      )

    runCatching { JsonSchemaValidator().ensure(schema, Jsons.deserialize("{\"test\": \"test\"}")) }
      .onFailure { assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(it) } }
  }

  @Test
  fun `test that it can handle job cancellation failures gracefully`() {
    val failureReason = "Could not find job with id: -1"
    assertThrows<StateConflictProblem>(JOB_NOT_RUNNING_DETAIL) {
      ConfigClientErrorHandler.handleError(RuntimeException(failureReason))
    }
  }

  @Test
  fun `test that it doesn't throw on non-error http responses`() {
    assertDoesNotThrow {
      ConfigClientErrorHandler.handleError(HttpResponse.ok<String>(), resourceId.toString())
    }
  }

  @Test
  fun `raw exception text is not returned`() {
    val ioProblem =
      assertThrows<UnprocessableEntityProblem> {
        ConfigClientErrorHandler.handleError(IOException("jdbc:postgresql://db:5432 refused"))
      }
    assertNull(ioProblem.problem.getData())
    assertEquals(UnprocessableEntityProblem().problem.getDetail(), ioProblem.problem.getDetail())

    val unexpected =
      assertThrows<UnexpectedProblem> {
        ConfigClientErrorHandler.handleError(RuntimeException("NPE at io.airbyte.Foo.bar(Foo.kt:12)"))
      }
    assertNull(unexpected.problem.getData())
    assertEquals(
      "An unexpected error occurred while processing the request. Retry; if it keeps failing, contact support and include the time of the request.",
      unexpected.problem.getDetail(),
    )

    val oauth =
      assertThrows<OAuthCallbackFailureProblem> {
        ConfigClientErrorHandler.handleError(OAuthCallbackException("invalid_grant: provider says no"))
      }
    assertNull(oauth.problem.getData())
    assertEquals(OAuthCallbackFailureProblem().problem.getDetail(), oauth.problem.getDetail())
  }

  @Test
  fun `connection creation IO failures preserve status without exception text`() {
    val request = ConnectionCreateRequest(sourceId = UUID.randomUUID(), destinationId = UUID.randomUUID())
    val problem = ConfigClientErrorHandler.handleCreateConnectionError(IOException("internal I/O error"), request)
    assertEquals(500, problem.problem.getStatus())
    assertNull(problem.problem.getData())
    assertEquals(UnexpectedProblem().problem.getDetail(), problem.problem.getDetail())
  }

  @Test
  fun `connection creation preserves safe validation detail without changing status`() {
    val request = ConnectionCreateRequest(sourceId = UUID.randomUUID(), destinationId = UUID.randomUUID())
    val messages =
      mapOf(
        "supply old or new schedule schema but not both" to "supply old or new schedule schema but not both.",
        "json schema validation failed when comparing the data to the json schema. \nErrors: $.name: is missing but it is required \nSchema: {}" to
          "The request body failed validation: $.name: is missing but it is required.",
        "" to UnexpectedProblem().problem.getDetail(),
        "   " to UnexpectedProblem().problem.getDetail(),
      )
    for ((message, expectedDetail) in messages) {
      val problem = ConfigClientErrorHandler.handleCreateConnectionError(JsonValidationException(message), request)
      assertEquals(500, problem.problem.getStatus())
      assertNull(problem.problem.getData())
      assertEquals(expectedDetail, problem.problem.getDetail())
    }
  }

  @Test
  fun `upstream response bodies are not returned for unhandled errors`() {
    val serverError =
      assertThrows<UnexpectedProblem> {
        ConfigClientErrorHandler.handleError(HttpResponse.serverError<Any>().body(mapOf("message" to "stack trace")), resourceId.toString())
      }
    assertNull(serverError.problem.getData())
    assertEquals(UnexpectedProblem().problem.getDetail(), serverError.problem.getDetail())

    val badRequest =
      assertThrows<BadRequestProblem> {
        ConfigClientErrorHandler.handleError(HttpResponse.badRequest<Any>().body("boom"), resourceId.toString())
      }
    assertNull(badRequest.problem.getData())
    assertEquals(BadRequestProblem().problem.getDetail(), badRequest.problem.getDetail())
  }

  @Test
  fun `platform conflict messages become detail`() {
    val response = HttpResponse.status<Any>(HttpStatus.CONFLICT).body(mapOf("message" to "Version '1.0' already exists"))
    val upstream = assertThrows<TryAgainLaterConflictProblem> { ConfigClientErrorHandler.handleError(response, resourceId.toString()) }
    assertEquals("Version '1.0' already exists.", upstream.problem.getDetail())
    assertNull(upstream.problem.getData())

    val valueConflict =
      assertThrows<TryAgainLaterConflictProblem> {
        ConfigClientErrorHandler.handleError(ValueConflictKnownException("Version '1.0' already exists"))
      }
    assertEquals(upstream.problem.getDetail(), valueConflict.problem.getDetail())
    assertNull(valueConflict.problem.getData())

    val conflict = assertThrows<StateConflictProblem> { ConfigClientErrorHandler.handleError(ConflictException("Connection is already running")) }
    assertEquals("Connection is already running.", conflict.problem.getDetail())
    assertNull(conflict.problem.getData())
  }

  @Test
  fun `validation detail omits boilerplate and schema for both overloads`() {
    val message =
      "json schema validation failed when comparing the data to the json schema. \n" +
        "Errors: $.name: is missing but it is required \nSchema: \n{internal schema}"
    val throwableProblem = assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(JsonValidationException(message)) }
    val response = HttpResponse.status<Any>(HttpStatus.UNPROCESSABLE_ENTITY).body(mapOf("message" to message))
    val responseProblem = assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(response, resourceId.toString()) }
    for (problem in listOf(throwableProblem, responseProblem)) {
      assertEquals("The request body failed validation: $.name: is missing but it is required.", problem.problem.getDetail())
      assertNull(problem.problem.getData())
    }
  }

  @Test
  fun `empty validation messages preserve schema defaults`() {
    for (message in listOf(
      null,
      "",
      "   ",
      "\nSchema: {}",
      "json schema validation failed when comparing the data to the json schema. \nErrors: \nSchema: {}",
    )) {
      assertNull(toDetail(message))
    }
    assertEquals("Already a sentence.", toDetail("Already a sentence."))
    for (status in listOf(HttpStatus.CONFLICT, HttpStatus.UNPROCESSABLE_ENTITY)) {
      for (body in listOf(emptyMap<String, Any>(), mapOf("message" to ""))) {
        val response = HttpResponse.status<Any>(status).body(body)
        val problem =
          assertThrows<io.airbyte.api.problems.AbstractThrowableProblem> {
            ConfigClientErrorHandler.handleError(response, resourceId.toString())
          }
        val expected = if (status == HttpStatus.CONFLICT) TryAgainLaterConflictProblem() else UnprocessableEntityProblem()
        assertEquals(expected.problem.getDetail(), problem.problem.getDetail())
        assertNull(problem.problem.getData())
      }
    }
  }

  @Test
  fun `illegal state errors use safe detail`() {
    val cancellation = assertThrows<StateConflictProblem> { ConfigClientErrorHandler.handleError(IllegalStateException("Failed to cancel job")) }
    assertEquals(JOB_NOT_RUNNING_DETAIL, cancellation.problem.getDetail())
    assertNull(cancellation.problem.getData())
    val generic = assertThrows<StateConflictProblem> { ConfigClientErrorHandler.handleError(IllegalStateException()) }
    assertEquals(StateConflictProblem().problem.getDetail(), generic.problem.getDetail())
    assertNull(generic.problem.getData())
  }
}
