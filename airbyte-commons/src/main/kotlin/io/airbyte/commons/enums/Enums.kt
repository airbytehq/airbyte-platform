/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.enums

/**
 * Convert an enum value from the type of one enum to the same value in the type of another enum.
 *
 * This is useful for converting from enums that are conceptually the same but are different java
 * types. e.g. if the same model has multiple generated java types, this method allows you to
 * convert between them.
 *
 * @receiver Enum applies to all enum types
 * @param T enum to convert to
 * @return enum value as type [T] if a matching enum exists, throws an [IllegalArgumentException] if no enum value matches
 */
inline fun <reified T : Enum<T>> Enum<*>.convertTo(): T = enumValueOf(this.name)

/**
 * Convert a list of enum values from the type onf one enum to the same value in the type of another enum.
 *
 * @receiver List<Enum> applies to all lists of Enums
 * @param T enum to convert to
 * @return list of enum values as type [T], if any enum cannot be matched, throws an [IllegalArgumentException]
 */
inline fun <reified T : Enum<T>> List<Enum<*>>.convertTo(): List<T> = this.map { it.convertTo<T>() }

/**
 * Convert a string to its values as an enum. Normalizes the name to get to a match.
 *
 * @receiver String to convert to an enum of type [T]
 * @return value as enum value [T] or null of not found
 */
inline fun <reified T : Enum<T>> String.toEnum(): T? = enumValues<T>().firstOrNull { normalizeName(it.name) == normalizeName(this) }

/**
 * Test if two enums are compatible to be converted between. To be compatible they must have the
 * same values.
 *
 * @param T1 type of enum 1
 * @param T2 type of enum 2
 * @return true if compatible. otherwise, false.
 */
inline fun <reified T1 : Enum<T1>, reified T2 : Enum<T2>> isCompatible(): Boolean {
  val enum1Values = enumValues<T1>().map { it.name }.toSet()
  val enum2Values = enumValues<T2>().map { it.name }.toSet()
  return enum1Values.size == enum2Values.size && enum1Values == enum2Values
}

/**
 * Normalizes a string for case-insensitive and punctuation-insensitive comparison.
 *
 * Converts the input to lowercase and removes all non-alphanumeric characters.
 * Useful for matching strings against enum names or other identifiers in a lenient way.
 *
 * Examples:
 * - "Light-Blue" becomes "lightblue"
 * - "DARK_RED" becomes "darkred"
 *
 * @param name the input string to normalize
 * @return a normalized version of the string with only lowercase letters and digits
 */
fun normalizeName(name: String): String = name.lowercase().replace(Regex("[^a-zA-Z0-9]"), "")
