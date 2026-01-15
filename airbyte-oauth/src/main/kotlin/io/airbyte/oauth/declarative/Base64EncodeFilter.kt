/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.lib.filter.Filter
import java.nio.charset.StandardCharsets
import java.util.Base64

/**
 * A Jinjava filter that encodes a string to Base64.
 *
 * This filter is used in declarative OAuth configurations to encode credentials
 * for HTTP Basic Authentication headers. For example:
 * ```
 * Authorization: "Basic {{ (client_id_value ~ ':' ~ client_secret_value) | b64encode }}"
 * ```
 */
open class Base64EncodeFilter : Filter {
  override fun getName(): String = "b64encode"

  /**
   * Encodes the given string to Base64 using UTF-8 encoding.
   *
   * @param value the input string to be encoded
   * @return the Base64 encoded string
   */
  protected open fun encode(value: String): String = Base64.getEncoder().encodeToString(value.toByteArray(StandardCharsets.UTF_8))

  /**
   * Filters the given object and encodes it to Base64 if it is a string.
   * If the object is not a string, it returns the object as is.
   *
   * @param object the object to be filtered
   * @param interpreter the Jinjava interpreter
   * @param arg additional arguments (not used in this implementation)
   * @return the Base64 encoded string if the object is a string, otherwise the original object
   * @throws RuntimeException if there is an error encoding the value
   */
  override fun filter(
    `object`: Any,
    interpreter: JinjavaInterpreter,
    vararg arg: String,
  ): Any {
    if (`object` is String) {
      val value = `object`
      try {
        return encode(value)
      } catch (e: Exception) {
        val errorMsg = String.format("Failed to encode value to Base64: `%s`", value)
        throw RuntimeException(errorMsg, e)
      }
    }
    return `object`
  }
}
