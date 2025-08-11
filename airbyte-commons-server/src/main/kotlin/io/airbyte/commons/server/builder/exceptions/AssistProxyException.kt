/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.builder.exceptions

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.server.errors.KnownException

class AssistProxyException(
  private var responseCode: Int,
  jsonBody: JsonNode,
) : KnownException(getStringFromResponse(jsonBody), getThrowableFromResponse(jsonBody), getDetailsFromResponse(jsonBody)) {
  override fun getHttpCode(): Int = responseCode
}

fun getStringFromResponse(jsonBody: JsonNode): String {
  if (jsonBody.has("message")) {
    return jsonBody.get("message").asText()
  }
  return "Unknown AI Assistant error"
}

fun getDetailsFromResponse(jsonBody: JsonNode): Map<String, Any> {
  if (jsonBody.has("details")) {
    return jsonBody
      .get("details")
      .fields()
      .asSequence()
      .associate { it.key to it.value }
  }
  return emptyMap()
}

fun getThrowableFromResponse(jsonBody: JsonNode): Throwable? {
  if (jsonBody.has("exceptionStack")) {
    val message = getStringFromResponse(jsonBody)
    val givenStack = jsonBody.get("exceptionStack")
    val givenClassName = jsonBody.get("exceptionClassName")?.asText() ?: "Python"
    val stackTrace = convertToStackTrace(givenStack, givenClassName) ?: return null

    val throwable = Throwable(message)
    throwable.stackTrace = stackTrace

    return throwable
  }
  return null
}

fun convertToStackTrace(
  exceptionStack: JsonNode,
  exceptionClassName: String,
): Array<StackTraceElement>? {
  if (!exceptionStack.isArray) return null

  // exceptionStack is an array of strings from python
  return exceptionStack
    .mapIndexed { index, stackLine ->
      val stackTraceParts = stackLine.asText().split(":")
      val (fileName, lineNumber, functionName) = parseStackTraceParts(stackTraceParts, index)
      StackTraceElement(exceptionClassName, functionName, fileName, lineNumber)
    }.toTypedArray()
}

private fun parseStackTraceParts(
  parts: List<String>,
  index: Int,
): Triple<String, Int, String> =
  when (parts.size) {
    3 -> Triple(parts[0], parseLineNumber(parts[1], index), parts[2])
    2 -> Triple(parts[0], parseLineNumber(parts[1], index), "unknown_function")
    1 -> Triple("unknown_file.py", index + 1, parts[0])
    else -> Triple("unknown_file.py", index + 1, "unknown_function")
  }

private fun parseLineNumber(
  lineNumber: String,
  index: Int,
): Int = lineNumber.toIntOrNull() ?: (index + 1)
