/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.assist

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.commons.server.builder.exceptions.AssistProxyException
import io.airbyte.commons.server.builder.exceptions.ConnectorBuilderException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.SocketTimeoutException

private val logger = KotlinLogging.logger {}

class AssistProxy(
  private val proxyConfig: AssistConfiguration,
) {
  fun post(
    path: String,
    jsonBody: JsonNode?,
  ): JsonNode {
    logger.info { "Calling Assist API with path: $path" }
    val connection = setupConnection(path)
    return executeRequest(connection, jsonBody)
  }

  fun postNoWait(
    path: String,
    jsonBody: JsonNode?,
  ) {
    logger.info { "Initiating non-blocking warming request to Assist API with path: $path" }
    val connection = setupConnection(path)

    // PROBLEM: HttpURLConnection is not adept at firing requests and not waiting for a response.
    // HACK: Set a very short timeout for the warming request and ignore the timeout exception.
    connection.connectTimeout = 500
    connection.readTimeout = 500

    Thread {
      try {
        executeRequest(connection, jsonBody, suppressTimeout = true)
      } catch (e: Exception) {
        logger.error(e) { "Error in background warming request" }
      } finally {
        logger.info { "Background warming request completed" }
      }
    }.start()
  }

  private fun setupConnection(path: String) =
    proxyConfig.getConnection(path).apply {
      requestMethod = "POST"
      setRequestProperty("Content-Type", "application/json")
      doOutput = true
      connectTimeout = 1 * 60 * 1000 // 1 minute
      readTimeout = 10 * 60 * 1000 // 10 minutes
    }

  private fun executeRequest(
    connection: HttpURLConnection,
    jsonBody: JsonNode?,
    suppressTimeout: Boolean = false,
  ): JsonNode {
    connection.outputStream.use { outputStream ->
      objectMapper.writeValue(outputStream, jsonBody)
    }

    val responseCode: Int
    val jsonResponse: JsonNode

    try {
      responseCode = connection.responseCode
      logger.info { "Assist API response code: $responseCode" }
      val inputStream =
        if (responseCode in 200..299) {
          connection.inputStream
        } else {
          connection.errorStream
        }

      jsonResponse =
        inputStream.use { inputStream ->
          InputStreamReader(inputStream, "utf-8").use { reader ->
            reader.readText().let {
              objectMapper.readTree(it)
            }
          }
        }
    } catch (e: IOException) {
      if (suppressTimeout && e is SocketTimeoutException) {
        logger.debug(e) { "Suppressed timeout error" }
        return objectMapper.createObjectNode()
      }

      throw ConnectorBuilderException("AI Assistant processing error", e)
    } finally {
      connection.disconnect()
    }

    if (responseCode !in 200..299) {
      throw AssistProxyException(responseCode, jsonResponse)
    }

    return jsonResponse
  }

  companion object {
    private val objectMapper = ObjectMapper()
  }
}
