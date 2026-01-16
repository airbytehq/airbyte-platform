/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

@file:JvmName("AssertEndpoint")

package io.airbyte.server

import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.exceptions.HttpClientResponseException
import org.junit.jupiter.api.Assertions.assertEquals

/**
 * Extension function which converts a [HttpRequest] into a [HttpStatus].
 *
 * @receiver HttpClient is the micronaut http client
 * @param request is the [HttpRequest] to send to the [HttpClient]
 * @return the [HttpStatus] of the [request], or an underlying Exception
 *
 * If you're looking for a way to handle the [HttpClientResponseException] that may be returned from the exchange call, see [statusException].
 */
internal fun <T> HttpClient.status(request: HttpRequest<T>): HttpStatus = toBlocking().exchange<T, Any>(request).status

/**
 * Extension function which converts a [HttpRequest] into a [HttpStatus].  Additionally, handles the [HttpClientResponseException] exception
 * converting it to the underlying [HttpStatus] which it contains.
 *
 * @receiver HttpClient is the micronaut http client
 * @param request is the [HttpRequest] to send to the [HttpClient]
 * @return the [HttpStatus] of the [request], or an underlying Exception
 */
internal fun <T> HttpClient.statusException(request: HttpRequest<T>): HttpStatus =
  runCatching {
    toBlocking().exchange<T, Any>(request).status
  }.fold(
    onSuccess = { it },
    onFailure = {
      if (it is HttpClientResponseException) {
        it.status
      } else {
        throw Exception("unsupported response exception class ${it::class.java}")
      }
    },
  )

/** assertStatus compares two [HttpStatus] values. */
fun assertStatus(
  expected: HttpStatus,
  actual: HttpStatus,
): Unit = assertEquals(expected, actual)
