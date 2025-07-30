/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.resources.Resources
import jakarta.inject.Singleton
import java.io.File
import java.io.IOException
import java.nio.file.Files

/**
 * OpenApiConfigHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class OpenApiConfigHandler {
  val file: File
    get() = tmpFile!!

  companion object {
    private var tmpFile: File? = null

    init {
      try {
        tmpFile = File.createTempFile("airbyte", "openapiconfig")
        tmpFile!!.deleteOnExit()
        Files.writeString(tmpFile!!.toPath(), Resources.read("config.yaml"))
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
    }
  }
}
