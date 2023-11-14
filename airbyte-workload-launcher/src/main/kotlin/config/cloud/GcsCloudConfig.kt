/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config.cloud

class GcsCloudConfig(val applicationCredentials: String, val bucket: String) {
  class Builder {
    private var applicationCredentials: String = ""
    private var bucket: String = ""

    fun withApplicationCredentials(applicationCredentials: String): Builder {
      this.applicationCredentials = applicationCredentials
      return this
    }

    fun withBucket(bucket: String): Builder {
      this.bucket = bucket
      return this
    }

    fun build(): GcsCloudConfig {
      return GcsCloudConfig(applicationCredentials, bucket)
    }
  }

  companion object {
    fun builder(): Builder {
      return Builder()
    }
  }
}
