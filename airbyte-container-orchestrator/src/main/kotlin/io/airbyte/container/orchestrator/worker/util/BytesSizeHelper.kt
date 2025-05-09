/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.util

object BytesSizeHelper {
  private const val KB = 1_024L
  private const val MB = KB * KB
  private const val GB = MB * KB
  private const val TB = GB * KB
  private const val PB = TB * KB

  fun byteCountToDisplaySize(size: Long): String =
    when {
      size > PB -> "${size / PB} PB"
      size > TB -> "${size / TB} TB"
      size > GB -> "${size / GB} GB"
      size > MB -> "${size / MB} MB"
      size > KB -> "${size / KB} KB"
      else -> "$size bytes"
    }
}
