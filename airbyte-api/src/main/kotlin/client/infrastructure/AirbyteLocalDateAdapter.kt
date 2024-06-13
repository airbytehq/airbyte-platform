/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client.infrastructure

import com.squareup.moshi.FromJson
import com.squareup.moshi.ToJson
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * Custom Moshi adapter to handle JSON responses that contain a [LocalDate] value
 * that is represented as an array in the JSON payload:
 * <p>
 *   <code>
 *     "releaseDate": [
 *         2023,
 *         12,
 *         16
 *     ],
 *   </code>
 * </p>
 *
 * This is currently required as there is some data type inconsistency between Java/Jackson server side
 * deserialization and the Kotlin/Moshi client-side serialization of responses.  Different data types
 * appear to be used in the generated JSON than what the client excepts when mixing the different technologies
 * used by the client and server code.  If/when everything is standardized on the same language and SerDe
 * library, this adapter may no longer be necessary.
 */
class AirbyteLocalDateAdapter {
  @ToJson
  fun toJson(value: LocalDate): String {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(value)
  }

  @FromJson
  fun fromJson(value: Any): LocalDate {
    val localDateString =
      if (value is List<*>) {
        value.joinToString(separator = "-", transform = { i ->
          if (i is Number) {
            i.toInt().toString()
          } else {
            i.toString()
          }
        })
      } else {
        value.toString()
      }
    return LocalDate.parse(localDateString, DateTimeFormatter.ofPattern("yyyy-M-d"))
  }
}
