/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.errorHandlers

import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.exc.ValueInstantiationException
import io.airbyte.api.server.problems.BadRequestProblem
import io.micronaut.context.annotation.Replaces
import io.micronaut.core.convert.exceptions.ConversionErrorException
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.server.exceptions.ConversionErrorHandler
import io.micronaut.http.server.exceptions.ExceptionHandler
import io.micronaut.http.server.exceptions.response.Error
import io.micronaut.http.server.exceptions.response.ErrorContext
import io.micronaut.http.server.exceptions.response.ErrorResponseProcessor
import jakarta.inject.Inject
import java.time.format.DateTimeParseException
import java.util.Optional

/**
 * Replaces the ConversionErrorHandler bean that micronaut ships with and allows us to more
 * gracefully handle errors. Specifically deserialization errors cause us to end up here. The only
 * difference between our conversion error handler and the default one is that we return a bad
 * request problem whereas the default returns a generic problem that isn't suitable to show our
 * users. Due to how and where the ConversionErrorException gets thrown in micronaut code, we're not
 * really able to get more information about the deserialization error here.
 */
@Replaces(ConversionErrorHandler::class)
class AirbyteConversionErrorHandler
  @Inject
  constructor(responseProcessor: ErrorResponseProcessor<*>) :
  ExceptionHandler<ConversionErrorException, HttpResponse<*>> {
    private val responseProcessor: ErrorResponseProcessor<*>

    init {
      this.responseProcessor = responseProcessor
    }

    override fun handle(
      request: HttpRequest<*>?,
      exception: ConversionErrorException,
    ): HttpResponse<*> {
      var message: String
      if (exception.cause is ValueInstantiationException) {
        val exceptionCast = exception.cause as ValueInstantiationException
        // Handles invalid enum values
        val field: String = exceptionCast.path[0].fieldName
        val originalMessage =
          if (exceptionCast.cause == null) "Incorrectly formatted request body - Incorrectly formatted enum value" else exceptionCast.cause!!.message
        message = String.format(originalMessage!!, field)
      } else if (exception.cause is InvalidFormatException) {
        val exceptionCast = exception.cause as InvalidFormatException
        // Handles invalid format for things like UUID (not enough characters, etc)
        val field: String = exceptionCast.path[0].fieldName
        val value = exceptionCast.value as String
        val type: String = exceptionCast.targetType.simpleName
        message = String.format("Invalid value for field '%s': '%s' is not a valid %s type", field, value, type)
      } else if (exception.cause is IllegalArgumentException) {
        val exceptionCast = exception.cause as IllegalArgumentException
        message = exceptionCast.message!!
        val noEnumConstant = "No enum constant"

        // Hack for making a nice error message when enum validation fails.
        // DefaultConversionService for String -> Enum calls Enum.valueOf which is different than how
        // conversion works via micronaut-jackson-databind
        // for POST bodies (the specific Enum Type's fromValue function).
        // Unedited, this error looks something like this: "No enum constant
        // io.airbyte.public_api_server.models.JobStatusEnum.test"
        // Due to limiations around the data we have at this point, we can't tell the user which param they
        // passed invalid data for.
        if (message.contains(noEnumConstant)) {
          message = message.replace("\\w+\\.".toRegex(), "").replace(noEnumConstant, "Invalid enum value: ")
        }
      } else if (exception.cause is DateTimeParseException) {
        val exceptionCast = exception.cause as DateTimeParseException
        message = String.format("Invalid datetime value passed: %s", exceptionCast.parsedString)
      } else {
        // If we've made it here, this is an error type we don't yet know how to handle.
        message = "Incorrectly formatted request body"
      }
      return responseProcessor.processResponse(
        ErrorContext.builder(request!!)
          .cause(BadRequestProblem(message))
          .error(
            object : Error {
              override fun getPath(): Optional<String> {
                return Optional.of('/'.toString() + exception.argument.name)
              }

              override fun getMessage(): String {
                return exception.message!!
              }
            },
          )
          .build(),
        HttpResponse.badRequest<Any>(),
      )
    }
  }
