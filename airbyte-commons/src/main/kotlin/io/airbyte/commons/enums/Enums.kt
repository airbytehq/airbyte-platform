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
 * Convert a string to its values as an enum.
 *
 * Note: the behavior of this differs from the java version, this matches on exact matches only (case and punctuation).
 *
 * @receiver String to convert to an enum of type [T]
 * @return value as enum value [T] or null of not found
 */
inline fun <reified T : Enum<T>> String.toEnum(): T? = runCatching { enumValueOf<T>(this) }.getOrNull()
