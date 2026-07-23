/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

internal fun normalizeScimAttributePath(
  path: String,
  expectedSchema: String,
): String {
  val prefix = "$expectedSchema:"
  return if (path.startsWithAsciiIgnoreCase(prefix)) path.substring(prefix.length) else path
}

internal fun String.asciiLowercaseOrNull(): String? {
  if (any { it.code > 0x7f }) return null
  return map { character -> if (character in 'A'..'Z') character + ('a' - 'A') else character }.joinToString("")
}

internal fun String.equalsAsciiIgnoreCase(other: String): Boolean =
  asciiLowercaseOrNull()?.let { left -> other.asciiLowercaseOrNull()?.let(left::equals) } ?: false

private fun String.startsWithAsciiIgnoreCase(prefix: String): Boolean =
  length >= prefix.length && substring(0, prefix.length).equalsAsciiIgnoreCase(prefix)
