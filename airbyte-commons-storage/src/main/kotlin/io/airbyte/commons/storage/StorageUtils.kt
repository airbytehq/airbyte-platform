/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

const val STRUCTURED_LOG_FILE_EXTENSION = ".json"
private val DATE_FORMAT: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")

/**
 * Builds the ID of the uploaded file.  This is typically the path in blob storage.
 *
 * @param baseId The base path/ID of the file location
 * @param timestamp A timestamp as a string for uniqueness
 * @param hostname The hostname of the machine executing this method
 * @param uniqueIdentifier A random UUID as a string for uniqueness
 * @return The field ID.
 */
fun createFileId(
  baseId: String,
  timestamp: String = LocalDateTime.now().format(DATE_FORMAT),
  hostname: String = InetAddress.getLocalHost().hostName,
  uniqueIdentifier: String = UUID.randomUUID().toString(),
): String {
  // Remove the leading/trailing "/" from the base storage ID if present to avoid duplicates in the storage ID
  return "${baseId.trim('/')}/${timestamp}_${hostname}_${uniqueIdentifier.replace("-", "")}$STRUCTURED_LOG_FILE_EXTENSION"
}
