/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config.cloud

class S3CloudConfig(val accessKey: String, val bucket: String, val region: String, val secretAccessKey: String) {
  class Builder {
    private var accessKey: String = ""
    private var bucket: String = ""
    private var region: String = ""
    private var secretAccessKey: String = ""

    fun withAccessKey(accessKey: String): Builder {
      this.accessKey = accessKey
      return this
    }

    fun withBucket(bucket: String): Builder {
      this.bucket = bucket
      return this
    }

    fun withRegion(region: String): Builder {
      this.region = region
      return this
    }

    fun withSecretAccessKey(secretAccessKey: String): Builder {
      this.secretAccessKey = secretAccessKey
      return this
    }

    fun build(): S3CloudConfig {
      return S3CloudConfig(accessKey, bucket, region, secretAccessKey)
    }
  }

  companion object {
    fun builder(): Builder {
      return Builder()
    }
  }
}
