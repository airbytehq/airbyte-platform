/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:JvmName("Slug")

package io.airbyte.commons.server.slug

import java.text.Normalizer

/**
 * Converts a string to a URL-friendly slug by:
 * - Normalizing unicode characters to ASCII
 * - Converting special characters from multiple languages (German, Nordic, Turkish, etc.)
 * - Replacing non-alphanumeric characters with hyphens
 * - Converting to lowercase
 *
 * @param s the input string to slugify
 * @return a URL-safe slug string
 */
fun slugify(s: String): String =
  if (s.isBlank()) {
    ""
  } else {
    normalize(replace(s.trim())).lowercase()
  }

private fun replace(s: String): String =
  s
    .toCharArray()
    .map { replacements[it] ?: it }
    .joinToString("")

private fun normalize(s: String): String =
  Normalizer
    .normalize(s, Normalizer.Form.NFKD)
    .let { PATTERN_NORMALIZE_NON_ASCII.replace(it, "") }
    .let { PATTERN_NORMALIZE_HYPHEN_SEPARATOR.replace(it, "-") }
    .let { PATTERN_NORMALIZE_TRIM_DASH.replace(it, "") }

private val PATTERN_NORMALIZE_NON_ASCII = "[^\\p{ASCII}]+".toRegex()
private val PATTERN_NORMALIZE_HYPHEN_SEPARATOR = "[\\W\\s+]+".toRegex()
private val PATTERN_NORMALIZE_TRIM_DASH = "^-|-$".toRegex()

// replacements are based on the previous slugify implementation
private val replacements: Map<Char, String> =
  buildMap {
    // German
    put('\u00c4', "Ae")
    put('\u00e4', "ae")
    put('\u00d6', "Oe")
    put('\u00f6', "oe")
    put('\u00dc', "Ue")
    put('\u00fc', "ue")
    put('\u00df', "ss")

    // Nordic
    put('\u00e5', "aa")
    put('\u00c5', "Aa")
    put('\u00e6', "ae")
    put('\u00c6', "Ae")
    put('\u00f8', "oe")
    put('\u00d8', "Oe")

    // Turkish
    put('\u011e', "g")
    put('\u011f', "g")
    put('\u0130', "i")
    put('\u0131', "i")
    put('\u015e', "s")
    put('\u015f', "s")

    // Cyrillic
    put('\u0410', "A")
    put('\u0411', "B")
    put('\u0412', "V")
    put('\u0413', "G")
    put('\u0414', "D")
    put('\u0415', "E")
    put('\u0416', "Zh")
    put('\u0417', "Z")
    put('\u0418', "I")
    put('\u0419', "J")
    put('\u041a', "K")
    put('\u041b', "L")
    put('\u041c', "M")
    put('\u041d', "N")
    put('\u041e', "O")
    put('\u041f', "P")
    put('\u0420', "R")
    put('\u0421', "S")
    put('\u0422', "T")
    put('\u0423', "U")
    put('\u0424', "F")
    put('\u0425', "H")
    put('\u0426', "Ts")
    put('\u0427', "Ch")
    put('\u0428', "Sh")
    put('\u0429', "Shch")
    put('\u042a', "'")
    put('\u042b', "Y")
    put('\u042c', "'")
    put('\u042d', "E")
    put('\u042e', "Yu")
    put('\u042f', "Ya")
    put('\u0430', "a")
    put('\u0431', "b")
    put('\u0432', "v")
    put('\u0433', "g")
    put('\u0434', "d")
    put('\u0435', "e")
    put('\u0436', "zh")
    put('\u0437', "z")
    put('\u0438', "i")
    put('\u0439', "j")
    put('\u043a', "k")
    put('\u043b', "l")
    put('\u043c', "m")
    put('\u043d', "n")
    put('\u043e', "o")
    put('\u043f', "p")
    put('\u0440', "r")
    put('\u0441', "s")
    put('\u0442', "t")
    put('\u0443', "u")
    put('\u0444', "f")
    put('\u0445', "h")
    put('\u0446', "ts")
    put('\u0447', "ch")
    put('\u0448', "sh")
    put('\u0449', "shch")
    put('\u044a', "'")
    put('\u044b', "y")
    put('\u044c', "'")
    put('\u044d', "e")
    put('\u044e', "yu")
    put('\u044f', "ya")

    // Polish
    put('\u0141', "L")
    put('\u0142', "l")

    // Greek
    put('\u0391', "A")
    put('\u0392', "B")
    put('\u0393', "G")
    put('\u0394', "D")
    put('\u0395', "E")
    put('\u0396', "Z")
    put('\u0397', "H")
    put('\u0398', "TH")
    put('\u0399', "I")
    put('\u039A', "K")
    put('\u039B', "L")
    put('\u039C', "M")
    put('\u039D', "N")
    put('\u039E', "KS")
    put('\u039F', "O")
    put('\u03A0', "P")
    put('\u03A1', "R")
    put('\u03A3', "S")
    put('\u03A4', "T")
    put('\u03A5', "Y")
    put('\u03A6', "F")
    put('\u03A7', "X")
    put('\u03A8', "PS")
    put('\u03A9', "W")

    put('\u03B1', "a")
    put('\u03B2', "b")
    put('\u03B3', "g")
    put('\u03B4', "d")
    put('\u03B5', "e")
    put('\u03B6', "z")
    put('\u03B7', "h")
    put('\u03B8', "th")
    put('\u03B9', "i")
    put('\u03BA', "k")
    put('\u03BB', "l")
    put('\u03BC', "m")
    put('\u03BD', "n")
    put('\u03BE', "ks")
    put('\u03BF', "o")
    put('\u03C0', "p")
    put('\u03C1', "r")
    put('\u03C2', "s")
    put('\u03C3', "s")
    put('\u03C4', "t")
    put('\u03C5', "y")
    put('\u03C6', "f")
    put('\u03C7', "x")
    put('\u03C8', "ps")
    put('\u03C9', "w")

    put('\u0386', "A")
    put('\u0388', "E")
    put('\u0389', "H")
    put('\u038A', "I")
    put('\u038C', "O")
    put('\u038E', "Y")
    put('\u038F', "W")

    put('\u03AC', "a")
    put('\u03AD', "e")
    put('\u03AE', "h")
    put('\u03CC', "o")
    put('\u03AF', "i")
    put('\u03CD', "y")
    put('\u03CE', "w")

    put('\u03AA', "I")
    put('\u03AB', "Y")
    put('\u03CA', "i")
    put('\u03CB', "u")
    put('\u03B0', "u")
    put('\u0390', "i")

    // Arabic
    put('\u0623', "a")
    put('\u0628', "b")
    put('\u062A', "t")
    put('\u062B', "th")
    put('\u062C', "g")
    put('\u062D', "h")
    put('\u062E', "kh")
    put('\u062F', "d")
    put('\u0630', "th")
    put('\u0631', "r")
    put('\u0632', "z")
    put('\u0633', "s")
    put('\u0634', "sh")
    put('\u0635', "s")
    put('\u0636', "d")
    put('\u0637', "t")
    put('\u0638', "th")
    put('\u0639', "aa")
    put('\u063A', "gh")
    put('\u0641', "f")
    put('\u0642', "k")
    put('\u0643', "k")
    put('\u0644', "l")
    put('\u0645', "m")
    put('\u0646', "n")
    put('\u0647', "h")
    put('\u0648', "o")
    put('\u064A', "y")
  }
