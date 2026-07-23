/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.duration

import kotlin.time.Duration.Companion.milliseconds

/**
 * formatMillis returns a human-readable string of a [milli] duration.
 *
 * The format is "D day(s) H hour(s) M minute(s) S second(s)".
 *
 * If the leading values or trailing values are 0, they will not be output.
 * For example, you would see "1 day" instead of "1 day 0 hours 0 minutes 0 seconds" and
 * "10 minutes 5 seconds" instead of "0 days 0 hours 10 minutes 5 seconds".
 *
 * If the non-leading/trailing values are 0, they will be output.  For example,  "1 day 0 hours 0 minutes 1 second".
 *
 * If the [milli] value is <= 0, then "0 seconds" will be returned.
 */
fun formatMilli(milli: Long): String {
  val result =
    milli.milliseconds.toComponents { days, hours, minutes, seconds, _ ->
      val parts = mutableListOf<String>()
      when {
        days == 1L -> parts.add("$days day")
        days > 1L -> parts.add("$days days")
      }
      when {
        hours == 1 -> parts.add("$hours hour")
        hours > 1 -> parts.add("$hours hours")
        hours == 0 && days > 0 && (minutes > 0 || seconds > 0) -> parts.add("0 hours")
      }
      when {
        minutes == 1 -> parts.add("$minutes minute")
        minutes > 1 -> parts.add("$minutes minutes")
        minutes == 0 && (days > 0 || hours > 0) && seconds > 0 -> parts.add("0 minutes")
      }
      when {
        seconds == 1 -> parts.add("$seconds second")
        seconds > 1 -> parts.add("$seconds seconds")
      }

      parts.joinToString(separator = " ")
    }

  return result.takeIf { it.isNotEmpty() } ?: "0 seconds"
}
