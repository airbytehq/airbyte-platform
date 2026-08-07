/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

sealed interface ScimFilter {
  data class Equal(
    val attribute: Attribute,
    val value: String,
    val caseSensitive: Boolean,
  ) : ScimFilter

  data class And(
    val clauses: List<Equal>,
  ) : ScimFilter

  enum class Attribute {
    USER_NAME,
    EXTERNAL_ID,
    EMAIL,
    WORK_EMAIL,
    DISPLAY_NAME,
    MEMBER,
  }
}

object ScimFilterParser {
  fun parseUser(filter: String): ScimFilter.And =
    failClosed {
      val parser = Parser(filter, SCIM_USER_SCHEMA)
      val clauses = mutableListOf(parser.parseUserClause())
      while (!parser.isFinished()) {
        parser.expectIdentifier("and", requireLeadingSpace = true)
        clauses += parser.parseUserClause(requireLeadingSpace = true)
      }
      ScimFilter.And(clauses)
    }

  fun parseGroup(filter: String): ScimFilter.Equal =
    failClosed {
      val parser = Parser(filter, SCIM_GROUP_SCHEMA)
      val result = parser.parseGroupClause()
      parser.expectFinished()
      result
    }

  private inline fun <T> failClosed(block: () -> T): T =
    try {
      block()
    } catch (exception: ScimException) {
      throw exception
    } catch (_: FilterParseException) {
      throw ScimErrors.invalidFilter()
    }
}

private class Parser(
  filter: String,
  resourceSchema: String,
) {
  private val lexer = Lexer(filter, resourceSchema)
  private var token = lexer.next()

  fun parseUserClause(requireLeadingSpace: Boolean = false): ScimFilter.Equal {
    val first = attributeIdentifier(requireLeadingSpace)
    return when {
      first.equalsAsciiIgnoreCase("userName") -> equality(ScimFilter.Attribute.USER_NAME, caseSensitive = false)
      first.equalsAsciiIgnoreCase("externalId") -> equality(ScimFilter.Attribute.EXTERNAL_ID, caseSensitive = true)
      first.equalsAsciiIgnoreCase("emails") -> parseEmailClause()
      else -> malformed()
    }
  }

  fun parseGroupClause(): ScimFilter.Equal {
    val first = attributeIdentifier()
    return when {
      first.equalsAsciiIgnoreCase("displayName") -> equality(ScimFilter.Attribute.DISPLAY_NAME, caseSensitive = false)
      first.equalsAsciiIgnoreCase("members") -> {
        expect(TokenType.LEFT_BRACKET)
        expectIdentifier("value")
        expectIdentifier("eq", requireLeadingSpace = true)
        val value = string(requireLeadingSpace = true)
        expect(TokenType.RIGHT_BRACKET)
        ScimFilter.Equal(ScimFilter.Attribute.MEMBER, value, caseSensitive = true)
      }
      else -> malformed()
    }
  }

  private fun parseEmailClause(): ScimFilter.Equal =
    when (token.type) {
      TokenType.DOT -> {
        expect(TokenType.DOT)
        expectIdentifier("value")
        equality(ScimFilter.Attribute.EMAIL, caseSensitive = false)
      }
      TokenType.LEFT_BRACKET -> {
        expect(TokenType.LEFT_BRACKET)
        expectIdentifier("type")
        expectIdentifier("eq", requireLeadingSpace = true)
        if (!string(requireLeadingSpace = true).equalsAsciiIgnoreCase("work")) {
          malformed()
        }
        expect(TokenType.RIGHT_BRACKET)
        expect(TokenType.DOT)
        expectIdentifier("value")
        equality(ScimFilter.Attribute.WORK_EMAIL, caseSensitive = false)
      }
      else -> malformed()
    }

  private fun equality(
    attribute: ScimFilter.Attribute,
    caseSensitive: Boolean,
  ): ScimFilter.Equal {
    expectIdentifier("eq", requireLeadingSpace = true)
    return ScimFilter.Equal(attribute, string(requireLeadingSpace = true), caseSensitive)
  }

  fun expectIdentifier(
    expected: String,
    requireLeadingSpace: Boolean = false,
  ) {
    if (!identifier(requireLeadingSpace).equalsAsciiIgnoreCase(expected)) {
      malformed()
    }
  }

  fun expectFinished() {
    if (!isFinished()) {
      malformed()
    }
  }

  fun isFinished(): Boolean = token.type == TokenType.END && token.leadingSpaces == 0

  private fun attributeIdentifier(requireLeadingSpace: Boolean = false): String {
    if (token.type != TokenType.SCHEMA_PREFIX) {
      return identifier(requireLeadingSpace)
    }
    requireLeadingSpaces(requireLeadingSpace)
    advance()
    return identifier()
  }

  private fun identifier(requireLeadingSpace: Boolean = false): String {
    if (token.type != TokenType.IDENTIFIER) {
      malformed()
    }
    requireLeadingSpaces(requireLeadingSpace)
    return token.value.also { advance() }
  }

  private fun string(requireLeadingSpace: Boolean = false): String {
    if (token.type != TokenType.STRING) {
      malformed()
    }
    requireLeadingSpaces(requireLeadingSpace)
    return token.value.also { advance() }
  }

  private fun expect(type: TokenType) {
    if (token.type != type || token.leadingSpaces != 0) {
      malformed()
    }
    advance()
  }

  private fun requireLeadingSpaces(required: Boolean) {
    if (token.leadingSpaces != if (required) 1 else 0) {
      malformed()
    }
  }

  private fun advance() {
    token = lexer.next()
  }
}

private class Lexer(
  private val source: String,
  private val resourceSchema: String,
) {
  private var index = 0

  fun next(): Token {
    val leadingSpaces = skipWhitespace()
    if (index == source.length) {
      return Token(TokenType.END, "", leadingSpaces)
    }
    val remaining = source.substring(index)
    val normalized = normalizeScimAttributePath(remaining, resourceSchema)
    if (normalized.length != remaining.length) {
      index += remaining.length - normalized.length
      return Token(TokenType.SCHEMA_PREFIX, remaining.substring(0, remaining.length - normalized.length), leadingSpaces)
    }
    return when (source[index]) {
      '.' -> single(TokenType.DOT, leadingSpaces)
      '[' -> single(TokenType.LEFT_BRACKET, leadingSpaces)
      ']' -> single(TokenType.RIGHT_BRACKET, leadingSpaces)
      '"' -> string(leadingSpaces)
      else -> identifier(leadingSpaces)
    }
  }

  private fun skipWhitespace(): Int {
    var spaces = 0
    while (index < source.length && source[index].isWhitespace()) {
      if (source[index] != ' ') {
        malformed()
      }
      spaces++
      index++
    }
    return spaces
  }

  private fun single(
    type: TokenType,
    leadingSpaces: Int,
  ): Token = Token(type, source[index++].toString(), leadingSpaces)

  private fun identifier(leadingSpaces: Int): Token {
    val start = index
    if (!source[index].isAsciiLetter()) {
      malformed()
    }
    index++
    while (index < source.length && (source[index].isAsciiLetter() || source[index].isAsciiDigit() || source[index] == '_' || source[index] == '-')) {
      index++
    }
    return Token(TokenType.IDENTIFIER, source.substring(start, index), leadingSpaces)
  }

  private fun string(leadingSpaces: Int): Token {
    index++
    val result = StringBuilder()
    while (index < source.length) {
      val character = source[index++]
      when {
        character == '"' -> return Token(TokenType.STRING, result.toString(), leadingSpaces)
        character == '\\' -> result.append(escape())
        character.code < 0x20 -> malformed()
        else -> result.append(character)
      }
    }
    malformed()
  }

  private fun escape(): Char {
    if (index == source.length) {
      malformed()
    }
    return when (val escaped = source[index++]) {
      '"', '\\', '/' -> escaped
      'b' -> '\b'
      'f' -> '\u000C'
      'n' -> '\n'
      'r' -> '\r'
      't' -> '\t'
      'u' -> unicodeEscape()
      else -> malformed()
    }
  }

  private fun unicodeEscape(): Char {
    if (index + 4 > source.length) {
      malformed()
    }
    val value = source.substring(index, index + 4).toIntOrNull(16) ?: malformed()
    index += 4
    return value.toChar()
  }
}

private data class Token(
  val type: TokenType,
  val value: String,
  val leadingSpaces: Int,
)

private enum class TokenType {
  IDENTIFIER,
  SCHEMA_PREFIX,
  STRING,
  DOT,
  LEFT_BRACKET,
  RIGHT_BRACKET,
  END,
}

private class FilterParseException : RuntimeException()

private fun malformed(): Nothing = throw FilterParseException()

private fun Char.isAsciiLetter(): Boolean = this in 'A'..'Z' || this in 'a'..'z'

private fun Char.isAsciiDigit(): Boolean = this in '0'..'9'
