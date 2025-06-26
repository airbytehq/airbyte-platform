/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors.handlers

import io.airbyte.commons.json.Jsons
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Produces
import io.micronaut.http.server.exceptions.ExceptionHandler
import jakarta.inject.Singleton
import org.openapitools.client.infrastructure.ClientException

/**
 * Handles unprocessable content exceptions.
 */
@Produces
@Singleton
class ClientExceptionHandler : ExceptionHandler<ClientException, HttpResponse<*>> {
  override fun handle(
    request: HttpRequest<*>,
    exception: ClientException,
  ): HttpResponse<*> =
    HttpResponse
      .status<Any>(HttpStatus.valueOf(exception.statusCode))
      .body(Jsons.serialize(MessageObject(exception.message)))
      .contentType(MediaType.APPLICATION_JSON)

  private data class MessageObject(
    val message: String?,
  )
}
