/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

data class FileTransferInformations
  constructor(
    @JsonProperty("file_url")
    var fileUrl: String,
    @JsonProperty("file_relative_path")
    var fileRelativePath: String,
    @JsonProperty("source_file_url")
    var sourceFileUrl: String,
    @JsonProperty("modified")
    var modified: String,
    @JsonProperty("bytes")
    var bytes: Long,
  ) : Serializable {
    fun withFileUrl(fileUrl: String): FileTransferInformations {
      this.fileUrl = fileUrl
      return this
    }

    fun withFileRelativePath(fileRelativePath: String): FileTransferInformations {
      this.fileRelativePath = fileRelativePath
      return this
    }

    fun withSourceFileUrl(sourceFileUrl: String): FileTransferInformations {
      this.sourceFileUrl = sourceFileUrl
      return this
    }

    fun withModified(modified: String): FileTransferInformations {
      this.modified = modified
      return this
    }

    fun withBytes(bytes: Long): FileTransferInformations {
      this.bytes = bytes
      return this
    }
  }
