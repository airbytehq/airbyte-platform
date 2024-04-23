package io.airbyte.server.apis.publicapi.errorHandlers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.ValueConflictKnownException
import io.airbyte.commons.server.errors.problems.BadRequestProblem
import io.airbyte.commons.server.errors.problems.ConflictProblem
import io.airbyte.commons.server.errors.problems.InvalidApiKeyProblem
import io.airbyte.commons.server.errors.problems.ResourceNotFoundProblem
import io.airbyte.commons.server.errors.problems.SyncConflictProblem
import io.airbyte.commons.server.errors.problems.UnexpectedProblem
import io.airbyte.commons.server.errors.problems.UnprocessableEntityProblem
import io.airbyte.config.persistence.ConfigNotFoundException
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpResponseFactory
import io.micronaut.http.HttpStatus
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.util.UUID

class ConfigClientErrorHandlerTest {
  private val resourceId = UUID.randomUUID()

  private val httpResponseFactory = HttpResponseFactory.INSTANCE

  @Test
  fun `test that it can handle errors for an HttpResponse`() {
    val notFoundResponse = HttpResponse.notFound("body")
    assertThrows<ResourceNotFoundProblem> { ConfigClientErrorHandler.handleError(notFoundResponse, resourceId.toString()) }

    val conflictResponse =
      httpResponseFactory.status<String>(HttpStatus.CONFLICT, "test")
        .body(mapOf("message" to "test"))
    assertThrows<SyncConflictProblem> { ConfigClientErrorHandler.handleError(conflictResponse, resourceId.toString()) }

    val unauthorizedResponse = httpResponseFactory.status<String>(HttpStatus.UNAUTHORIZED)
    assertThrows<InvalidApiKeyProblem> { ConfigClientErrorHandler.handleError(unauthorizedResponse, resourceId.toString()) }

    val unprocessibleEntityResponse =
      httpResponseFactory.status<String>(HttpStatus.UNPROCESSABLE_ENTITY, "test")
        .body(mapOf("message" to "test"))
    assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(unprocessibleEntityResponse, resourceId.toString()) }

    val badRequestResponse = httpResponseFactory.status<String>(HttpStatus.BAD_REQUEST, "test")
    assertThrows<BadRequestProblem> { ConfigClientErrorHandler.handleError(badRequestResponse, resourceId.toString()) }

    val unexpectedResponse = httpResponseFactory.status<String>(HttpStatus.INTERNAL_SERVER_ERROR, "test")
    assertThrows<UnexpectedProblem> { ConfigClientErrorHandler.handleError(unexpectedResponse, resourceId.toString()) }
  }

  @Test
  fun `test that it can handle throwables`() {
    assertThrows<ResourceNotFoundProblem> { ConfigClientErrorHandler.handleError(ConfigNotFoundException("test", "test"), resourceId.toString()) }

    assertThrows<SyncConflictProblem> { ConfigClientErrorHandler.handleError(ValueConflictKnownException("test"), resourceId.toString()) }

    assertThrows<ConflictProblem> { ConfigClientErrorHandler.handleError(IllegalStateException(), resourceId.toString()) }

    assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(JsonValidationException("test"), resourceId.toString()) }
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
      .onFailure { assertThrows<UnprocessableEntityProblem> { ConfigClientErrorHandler.handleError(it, resourceId.toString()) } }
  }

  @Test
  fun `test that it can handle job cancellation failures gracefully`() {
    val failureReason = "Could not find job with id: -1"
    assertThrows<ConflictProblem>(JOB_NOT_RUNNING_MESSAGE) {
      ConfigClientErrorHandler.handleError(RuntimeException(failureReason), resourceId.toString())
    }
  }

  @Test
  fun `test that it doesn't throw on non-error http responses`() {
    assertDoesNotThrow {
      ConfigClientErrorHandler.handleError(HttpResponse.ok<String>(), resourceId.toString())
    }
  }
}
