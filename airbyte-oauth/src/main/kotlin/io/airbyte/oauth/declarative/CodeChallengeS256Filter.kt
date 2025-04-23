/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative

import com.hubspot.jinjava.interpret.JinjavaInterpreter
import com.hubspot.jinjava.lib.filter.Filter
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

open class CodeChallengeS256Filter : Filter {
  override fun getName(): String = "codechallengeS256"

  /**
   * Generates a code challenge using the SHA-256 hashing algorithm. The input value is hashed and
   * then encoded to a Base64 string.
   *
   * @param value the input string to be hashed and encoded
   * @return the Base64 encoded SHA-256 hash of the input value
   * @throws Exception if the SHA-256 algorithm is not available
   */
  @Throws(Exception::class)
  protected open fun getCodeChallenge(value: String): String {
    val digest = MessageDigest.getInstance("SHA-256")
    return Base64.getEncoder().encodeToString(digest.digest(value.toByteArray(StandardCharsets.UTF_8)))
  }

  /**
   * Filters the given object and generates a code challenge using the S256 method if the object is a
   * string. If the object is not a string, it returns the object as is.
   *
   * @param object the object to be filtered
   * @param interpreter the Jinjava interpreter
   * @param arg additional arguments (not used in this implementation)
   * @return the code challenge if the object is a string, otherwise the original object
   * @throws RuntimeException if there is an error generating the code challenge
   */
  override fun filter(
    `object`: Any,
    interpreter: JinjavaInterpreter,
    vararg arg: String,
  ): Any {
    if (`object` is String) {
      val value = `object`
      try {
        return getCodeChallenge(value)
      } catch (e: Exception) {
        val errorMsg = String.format("Failed to get `codechallengeS256` from: `%s`", value)
        throw RuntimeException(errorMsg, e)
      }
    }
    return `object`
  }
}
